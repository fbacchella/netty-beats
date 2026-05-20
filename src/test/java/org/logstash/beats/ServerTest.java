package org.logstash.beats;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.logstash.beats.BeatsParser.InvalidFrameProtocolException;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerTest {

    private int randomPort;
    private EventLoopGroup group;
    private final String host = "0.0.0.0";

    @BeforeEach
    void setUp() {
        randomPort = tryGetPort();
        group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
    }

    @AfterEach
    void shutdown() {
        group.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
    }

    @Test
    void testServerShouldTerminateConnectionWhenExceptionHappen() throws InterruptedException, ExecutionException {
        int inactivityTime = 3; // in seconds
        int concurrentConnections = 10;

        AtomicInteger connected = new AtomicInteger(0);

        CountDownLatch latch = new CountDownLatch(concurrentConnections);

        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setClientInactivityTimeout(inactivityTime)
                        .setShutdownDelay(100, TimeUnit.MILLISECONDS)
                        .setChannelClass(NioServerSocketChannel.class);

        final AtomicBoolean otherCause = new AtomicBoolean(false);
        server.setMessageListener(new MessageListener() {
            @Override
            public void onNewConnection(ChannelHandlerContext ctx) {
                // Make sure connection is closed on exception too.
                if (connected.incrementAndGet() == 1) {
                    throw new RuntimeException("Dummy");
                }
            }

            @Override
            public void onConnectionClose(ChannelHandlerContext ctx) {
                latch.countDown();
            }

            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
                // Make sure connection is closed on exception too.
                throw new RuntimeException("Dummy");
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                // Make sure only intended exception is thrown
                if (!"Dummy".equals(cause.getMessage())) {
                    otherCause.set(true);
                }
            }
        });

        Thread thread = new Thread(() -> serverRun(server));
        thread.start();
        server.f.get();

        try {
            ChannelFutureListener cfl = this::simpleSend;
            for (int i = 0; i < concurrentConnections; i++) {
                connectClient().addListener(cfl);
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertFalse(otherCause.get());
        } finally {
            server.stop();
            thread.join();
        }
    }

    @Test
    @Timeout(10)
    void testOverSizedBatch() throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger lastGoodCount = new AtomicInteger();
        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setMaxPayloadSize(100)
                        .setClientInactivityTimeout(1)
                        .setShutdownDelay(100, TimeUnit.MILLISECONDS)
                        .setChannelClass(NioServerSocketChannel.class);

        server.setMessageListener(new MessageListener() {
            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
                lastGoodCount.set(message.getData().size());
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                latch.countDown();
            }
        });

        Thread thread = new Thread(() -> serverRun(server));
        thread.start();
        server.f.get();

        ChannelFutureListener incrementSender = nf -> this.incrementSend(nf, 1);

        try {
            ChannelFuture cf = connectClient();
            cf.addListener(incrementSender);
            assertTrue(latch.await(1, TimeUnit.SECONDS));
            assertEquals(6, lastGoodCount.get());
        } finally {
            server.stop();
            thread.join();
        }
    }

    private void incrementSend(ChannelFuture f, int count) throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            batch.setBatchSize(1);
            ByteBuf contents = V2BatchTest.messageContents(count);
            batch.addMessage(0, contents, contents.readableBytes());
            ChannelFuture cf = f.channel().writeAndFlush(batch);
            if (count < 10 && f.channel().isOpen()) {
                ChannelFutureListener incrementSender = nf -> this.incrementSend(nf, count + 1);
                cf.addListener(incrementSender);
            } else {
                cf.channel().close();
            }
        }
    }

    @Test
    @Timeout(10)
    void testServerShouldTerminateConnectionIdleForTooLong() throws InterruptedException, ExecutionException {
        int inactivityTime = 3; // in seconds
        int concurrentConnections = 10;

        CountDownLatch latch = new CountDownLatch(concurrentConnections);
        AtomicBoolean exceptionClose = new AtomicBoolean(false);
        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setShutdownDelay(100, TimeUnit.MILLISECONDS)
                        .setClientInactivityTimeout(inactivityTime);
        server.setMessageListener(new MessageListener() {
            @Override
            public void onConnectionClose(ChannelHandlerContext ctx) {
                latch.countDown();
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                exceptionClose.set(true);
            }
        });

        Thread thread = new Thread(() -> serverRun(server));
        thread.start();
        server.f.get();

        try {
            long started = System.currentTimeMillis();

            for (int i = 0; i < concurrentConnections; i++) {
                connectClient();
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));

            long ended = System.currentTimeMillis();

            long diff = ended - started;
            assertEquals(inactivityTime, diff / 1000.0, 0.5);
            assertFalse(exceptionClose.get());
        } finally {
            server.stop();
            thread.join();
        }
    }

    @Test
    @Timeout(10)
    void testServerShouldAcceptConcurrentConnection() throws InterruptedException, ExecutionException {
        // Each connection is sending 1 batch.
        int concurrentConnections = 5;

        CountDownLatch latch = new CountDownLatch(concurrentConnections);
        CountDownLatch startLatch = new CountDownLatch(1);

        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setClientInactivityTimeout(30);

        server.setMessageListener(new MessageListener() {
            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
                latch.countDown();
            }
        });

        new Thread(() -> serverRun(server)).start();
        server.f.get();

        ChannelFutureListener opCompleted = f -> operationComplete(startLatch, f);
        for (int i = 0; i < concurrentConnections; i++) {
            new Thread(() -> connect(startLatch, opCompleted)).start();
        }

        startLatch.countDown();
        latch.await();
        server.stop();
        // No tests, releasing the latch is the expected result.
    }

    private void operationComplete(CountDownLatch startLatch, ChannelFuture future) throws InvalidFrameProtocolException, InterruptedException {
        startLatch.await();
        simpleSend(future);
        future.channel().close();
    }

    private void connect(CountDownLatch startLatch, ChannelFutureListener opCompleted) {
        try {
            startLatch.await();
            connectClient().addListener(opCompleted);
        } catch (InterruptedException e) {
            throw new CompletionException(e);
        }
    }

    public ChannelFuture connectClient() {
        Bootstrap b = new Bootstrap();
        b.group(group)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new BatchEncoder());
            }
        });
        return b.connect("localhost", randomPort);
    }

    private void simpleSend(ChannelFuture f) throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            batch.setBatchSize(1);
            ByteBuf contents = V2BatchTest.messageContents();
            batch.addMessage(1, contents, contents.readableBytes());
            f.channel().writeAndFlush(batch);
        }
    }

    /**
     * Try to find a random available port
     * @return an available listen port
     */
    private static int tryGetPort() {
        try (ServerSocket ss = new ServerSocket(0)){
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            return -1;
        }
    }

    private void serverRun(Server s) {
        try {
            s.listen();
        } catch (InterruptedException e) {
            // Ignored
        }
    }

}

package org.logstash.beats;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class BeatsHandler extends SimpleChannelInboundHandler<Batch> {

    private static final Logger logger = LogManager.getLogger();
    private static final int MAX_CAUSE_NESTING = 10;

    private final AtomicBoolean isQuietPeriod = new AtomicBoolean(false);

    private final IMessageListener messageListener;
    private ChannelHandlerContext context;

    public BeatsHandler(IMessageListener listener) {
        messageListener = listener;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        context = ctx;
        logger.trace("{} Channel Active", this::logPrefix);
        super.channelActive(ctx);
        messageListener.onNewConnection(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        logger.trace("{} Channel Inactive", this::logPrefix);
        messageListener.onConnectionClose(ctx);
    }


    @Override
    public void channelRead0(ChannelHandlerContext ctx, Batch batch) {
        logger.debug("{} Received a new payload", this::logPrefix);
        try {
            if (isQuietPeriod.get()) {
                logger.debug("{} Received batch but no executors available, ignoring...", this::logPrefix);
            } else {
                processBatchAndSendAck(ctx, batch);
            }
        } finally {
            //this channel is done processing this payload, instruct the connection handler to stop sending TCP keep alive
            ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().set(false);
            logger.debug("{}: batches pending: {}",
                         () -> ctx.channel().id().asShortText(),
                         () -> ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).get().get());
            batch.release();
            ctx.flush();
        }
    }

    /*
     * Do not propagate the SSL handshake exception down to the ruby layer handle it locally instead and close the connection
     * if the channel is still active. Calling `onException` will flush the content of the codec's buffer, this call
     * may block the thread in the event loop until completion, this should only affect LS 5 because it still supports
     * the multiline codec, v6 drop support for buffering codec in the beats input.
     *
     * For v5, I cannot drop the content of the buffer because this will create data loss because multiline content can
     * overlap Filebeat transmission; we were recommending multiline at the source in v5 and in v6 we enforce it.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            if (!(cause instanceof SSLHandshakeException)) {
                messageListener.onException(ctx, cause);
            }
            if (isNoisyException(cause)) {
                logger.info("{} closing{}", this::logPrefix, () -> logger.isDebugEnabled() ?  " (%s)".formatted(cause.getMessage()) : "");
            } else {
                Throwable realCause = extractCause(cause, 0);
                logger.atInfo()
                      .withThrowable(logger.isDebugEnabled() ? cause : null)
                      .log("{} Handling exception: {} (caused by: {})", this::logPrefix, () -> cause, () -> realCause);
                // when execution tasks rejected, no need to forward the exception to netty channel handlers
                if (cause instanceof RejectedExecutionException) {
                    // we no longer have event executors available since they are terminated, mostly by shutdown process
                    if (Objects.nonNull(cause.getMessage()) && cause.getMessage().contains("event executor terminated")) {
                        isQuietPeriod.compareAndSet(false, true);
                    }
                } else {
                    super.exceptionCaught(ctx, cause);
                }
            }
        } finally {
            ctx.flush();
            ctx.close();
        }
    }

    private void processBatchAndSendAck(ChannelHandlerContext ctx, Batch batch) {
        if (batch.isEmpty()) {
            logger.debug("Sending 0-seq ACK for empty batch");
            writeAck(ctx, batch.getProtocol(), 0);
        }
        for (Message message : batch) {
            logger.debug("{} Sending a new message for the listener, sequence: {}", this::logPrefix, message::getSequence);
            messageListener.onNewMessage(ctx, message);

            if (needAck(message)) {
                logger.trace("{} Acking message number {}", this::logPrefix, message::getSequence);
                writeAck(ctx, message.getBatch().getProtocol(), message.getSequence());
            }
        }
    }

    private boolean isNoisyException(Throwable ex) {
        return ex instanceof IOException && "Connection reset by peer".equals(ex.getMessage());
    }

    private boolean needAck(Message message) {
        return message.getSequence() == message.getBatch().getHighestSequence();
    }

    private void writeAck(ChannelHandlerContext ctx, byte protocol, int sequence) {
        ctx.writeAndFlush(new Ack(protocol, sequence));
    }

    /*
     * There is no easy way in Netty to support MDC directly,
     * we will use similar logic than Netty's LoggingHandler
     */
    private String logPrefix() {
        SocketAddress local = context.channel().localAddress();
        SocketAddress remote = context.channel().remoteAddress();

        String localhost = addressToString(local);
        String remotehost = addressToString(remote);

        return "[local: " + localhost + ", remote: " + remotehost + "] ";
    }

    private String addressToString(SocketAddress saddr) {
        if (saddr instanceof InetSocketAddress inetaddr) {
            return inetaddr.getAddress().getHostAddress() + ":" + inetaddr.getPort();
        } else {
            return saddr.toString();
        }
    }

    private Throwable extractCause(Throwable ex, int nesting) {
        Throwable cause = ex.getCause();
        if (cause == null || cause == ex) {
            return ex;
        } else if (nesting >= MAX_CAUSE_NESTING) {
            return cause; // do not recurse infinitely
        } else {
            return extractCause(cause, nesting + 1);
        }
    }

}

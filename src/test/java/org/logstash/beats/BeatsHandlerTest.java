package org.logstash.beats;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by ph on 2016-06-01.
 */
class BeatsHandlerTest {
    private static final SecureRandom randomizer = new SecureRandom();
    private SpyListener spyListener;
    private final int startSequenceNumber = randomizer.nextInt(100);
    private final int messageCount = 5;
    private V1Batch batch;

    private static class SpyListener implements IMessageListener {
        private boolean onNewConnectionCalled = false;
        private boolean onConnectionCloseCalled = false;
        private final List<Message> lastMessages = new ArrayList<>();

        @Override
        public void onNewMessage(ChannelHandlerContext ctx, Message message) {
            lastMessages.add(message);
        }

        @Override
        public void onNewConnection(ChannelHandlerContext ctx) {
            ctx.channel().attr(ConnectionHandler.CHANNEL_SEND_KEEP_ALIVE).set(new AtomicBoolean(false));
            onNewConnectionCalled = true;
        }

        @Override
        public void onConnectionClose(ChannelHandlerContext ctx) {
            onConnectionCloseCalled = true;
        }

        @Override
        public void onException(ChannelHandlerContext ctx, Throwable cause) {
        }

        @Override
        public void onChannelInitializeException(ChannelHandlerContext ctx, Throwable cause) {
        }

        public boolean isOnNewConnectionCalled() {
            return onNewConnectionCalled;
        }

        public boolean isOnConnectionCloseCalled() {
            return onConnectionCloseCalled;
        }

        public List<Message> getLastMessages() {
            return lastMessages;
        }

    }

    @BeforeEach
    void setup() {
        spyListener = new SpyListener();
        batch = new V1Batch();
        batch.setBatchSize(messageCount);
        for (int i = 0;i < messageCount;i++) {
            Message message = new Message(i + startSequenceNumber, Collections.emptyMap());
            batch.addMessage(message);
        }
    }

    @Test
    void testItCalledOnNewConnectionOnListenerWhenHandlerIsAdded() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);

        assertTrue(spyListener.isOnNewConnectionCalled());
        embeddedChannel.close();
    }

    @Test
    void testItCalledOnConnectionCloseOnListenerWhenChannelIsRemoved() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);
        embeddedChannel.close();

        assertTrue(spyListener.isOnConnectionCloseCalled());
    }

    @Test
    void testIsCallingNewMessageOnEveryMessage() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);

        assertEquals(messageCount, spyListener.getLastMessages().size());
        embeddedChannel.close();
    }

    @Test
    void testAcksLastMessageInBatch() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(batch);
        assertEquals(messageCount, spyListener.getLastMessages().size());
        Ack ack = embeddedChannel.readOutbound();
        assertEquals(Protocol.VERSION_1, ack.protocol());
        assertEquals(startSequenceNumber + messageCount - 1, ack.sequence());
        embeddedChannel.close();
    }

    @Test
    void testAcksZeroSequenceForEmptyBatch() {
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(new BeatsHandler(spyListener));
        embeddedChannel.writeInbound(new V2Batch());
        assertEquals(0, spyListener.getLastMessages().size());
        Ack ack = embeddedChannel.readOutbound();
        assertEquals(Protocol.VERSION_2, ack.protocol());
        assertEquals(0, ack.sequence());
        embeddedChannel.close();
    }

}

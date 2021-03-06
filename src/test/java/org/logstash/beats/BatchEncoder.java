package org.logstash.beats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.util.Map;

/**
 *  This Class is mostly used in the test suite to make the right assertions with the encoded data frame.
 *  This class support creating v1 or v2 lumberjack frames.
 *
 */
public class BatchEncoder extends MessageToByteEncoder<Batch> {

    private final static Logger logger = LogManager.getLogger(BatchEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, Batch batch, ByteBuf out) throws Exception {
        out.writeByte(batch.getProtocol());
        out.writeByte(Protocol.CODE_WINDOW_SIZE);
        out.writeInt(batch.size());

        ByteBuf buffer = getPayload(ctx, batch);

        try {
            out.writeBytes(buffer);
        } finally {
            buffer.release();
        }
    }

    protected ByteBuf getPayload(ChannelHandlerContext ctx, Batch batch) throws IOException {
        ByteBuf payload = ctx.alloc().buffer();
        ObjectWriter jsonWriter = DefaultJson.getWriterInstance();
        // Aggregates the payload that we could decide to compress or not.
        for (Message message : batch) {
            if (batch.getProtocol() == Protocol.VERSION_2) {
                encodeMessageWithJson(payload, message, jsonWriter);
            } else {
                encodeMessageWithFields(payload, message);
            }
        }
        return payload;
    }

    private void encodeMessageWithJson(ByteBuf payload, Message message, ObjectWriter jsonWriter) throws JsonProcessingException {
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte(Protocol.CODE_JSON_FRAME);
        payload.writeInt(message.getSequence());

        byte[] json = jsonWriter.writeValueAsBytes(message.getData());
        payload.writeInt(json.length);
        payload.writeBytes(json);
    }

    private void encodeMessageWithFields(ByteBuf payload, Message message) {
        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte(Protocol.CODE_FRAME);
        payload.writeInt(message.getSequence());
        payload.writeInt(message.getData().size());

        for (Map.Entry<?, ?> e : message.getData().entrySet()) {
            byte[] key = ((String) e.getKey()).getBytes();
            byte[] value = ((String) e.getValue()).getBytes();

            logger.debug("New entry: key: " + key + ", value: " + value);

            payload.writeInt(key.length);
            payload.writeBytes(key);
            payload.writeInt(value.length);
            payload.writeBytes(value);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    }

}

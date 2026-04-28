package org.logstash.beats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectReader;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class BeatsParser extends ByteToMessageDecoder {

    private static final Logger logger = LogManager.getLogger();

    private Batch batch;

    private enum States {
        READ_HEADER(1),
        READ_FRAME_TYPE(1),
        READ_WINDOW_SIZE(4),
        READ_JSON_HEADER(8),
        READ_COMPRESSED_FRAME_HEADER(4),
        READ_COMPRESSED_FRAME(-1), // -1 means the length to read is variable and defined in the frame itself.
        READ_JSON(-1),
        READ_DATA_FIELDS(-1);

        private final int length;

        States(int length) {
            this.length = length;
        }

    }

    private States currentState = States.READ_HEADER;
    private int requiredBytes = States.READ_HEADER.length;
    private int sequence = 0;
    private boolean decodingCompressedBuffer = false;

    private final int maxPayloadSize;
    private final ObjectReader jsonReader;
    private final Inflater inflater = new Inflater();

    /**
     * Create a beats parser with no maximum payload size check.
     */
    public BeatsParser() {
        maxPayloadSize = Integer.MAX_VALUE;
        jsonReader = DefaultJson.get();
    }

    /**
     * Create a parser with a maximum payload size. If the value is less than 0, it's not checked.
     * 
     * @param maxPayloadSize the maximum payload size
     */
    public BeatsParser(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize >= 0 ? maxPayloadSize : Integer.MAX_VALUE;
        jsonReader = DefaultJson.get();
    }

    /**
     * Create a parser with a maximum payload size and a non-default JSON reader. Any value less or equal to 0 for the max payload disable check.
     *
     * @param maxPayloadSize the maximum payload size
     * @param jsonReader a custom {@link ObjectReader}
     */
    public BeatsParser(int maxPayloadSize, ObjectReader jsonReader) {
        this.maxPayloadSize = maxPayloadSize >= 0 ? maxPayloadSize : Integer.MAX_VALUE;
        this.jsonReader = jsonReader;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws InvalidFrameProtocolException, IOException {
        try {
            if (!hasEnoughBytes(in)) {
                if (decodingCompressedBuffer){
                    throw new InvalidFrameProtocolException("Insufficient bytes in compressed content to decode: " + currentState);
                } else {
                    return;
                }
            }

            switch (currentState) {
            case READ_HEADER -> {
                logger.trace("Running: READ_HEADER");
                decodeHeader(in);
            }
            case READ_FRAME_TYPE -> {
                logger.trace("Running: READ_FRAME_TYPE");
                decodeFrameType(in);
            }
            case READ_WINDOW_SIZE -> {
                logger.trace("Running: READ_WINDOW_SIZE");
                decodeWindowSize(in, out);
            }
            case READ_DATA_FIELDS -> {
                logger.trace("Running: READ_DATA_FIELDS");
                decodeDataFields(in, out);
            }
            case READ_JSON_HEADER -> {
                logger.trace("Running: READ_JSON_HEADER");
                decodeJsonHeader(in);
            }
            case READ_COMPRESSED_FRAME_HEADER -> {
                logger.trace("Running: READ_COMPRESSED_FRAME_HEADER");
                decodeCompressedFrameHeader(in);
            }
            case READ_COMPRESSED_FRAME -> {
                logger.trace("Running: READ_COMPRESSED_FRAME");
                inflateCompressedFrame(ctx, in, out);
            }
            case READ_JSON -> {
                logger.trace("Running: READ_JSON");
                decodeJson(in, out);
            }
            }
        } catch (InvalidFrameProtocolException | RuntimeException | IOException e) {
            resetOnError(in, out);
            throw e;
        }
    }

    private void decodeHeader(ByteBuf in) throws InvalidFrameProtocolException {
        byte currentVersion = in.readByte();
        if (batch == null) {
            if (Protocol.isVersion2(currentVersion)) {
                logger.trace("Frame version 2 detected");
                batch = new V2Batch(maxPayloadSize, jsonReader);
            } else if (Protocol.isVersion1(currentVersion)) {
                logger.trace("Frame version 1 detected");
                batch = new V1Batch();
            } else {
                throw new InvalidFrameProtocolException("Unsupported protocol version: " + currentVersion);
            }
        }
        transition(States.READ_FRAME_TYPE);
    }

    private void decodeFrameType(ByteBuf in) throws InvalidFrameProtocolException {
        byte frameType = in.readByte();
        switch (frameType) {
            case Protocol.CODE_WINDOW_SIZE -> transition(States.READ_WINDOW_SIZE);
            case Protocol.CODE_JSON_FRAME -> transition(States.READ_JSON_HEADER);
            case Protocol.CODE_COMPRESSED_FRAME -> transition(States.READ_COMPRESSED_FRAME_HEADER);
            case Protocol.CODE_FRAME -> transition(States.READ_DATA_FIELDS);
            default -> throw new InvalidFrameProtocolException("Invalid Frame Type, received: " + frameType);
        }
    }

    private void decodeWindowSize(ByteBuf in, List<Object> out) throws InvalidFrameProtocolException {
        int batchSize = tryReadUnsigned(in, "Invalid window size", true);
        batch.setBatchSize(batchSize);
        logger.debug("New window size is {}", batchSize);

        if (!batch.isEmpty()) {
            logger.warn("New window size received but the current batch was not complete, sending the current batch");
            batchComplete(out);
        } else if (batch.getBatchSize() == 0) {
            logger.debug("New window size 0 received, sending an empty batch");
            out.add(batch);
            batchComplete(out);
        }

        transition(States.READ_HEADER);
    }

    private void decodeDataFields(ByteBuf in, List<Object> out) throws InvalidFrameProtocolException {
        sequence = tryReadUnsigned(in, "Invalid sequence number", true);
        int fieldsCount = tryReadUnsigned(in, "Invalid number of fields", false);

        if (fieldsCount * 4 > maxPayloadSize) {
            throw new InvalidFrameProtocolException("Oversized entry count: " + fieldsCount);
        }

        Map<String, String> dataMap = HashMap.newHashMap(fieldsCount);

        long currentPayload = 0;
        for (int count = 0; count < fieldsCount; count++) {
            currentPayload += 32;
            int fieldLength = tryReadUnsigned(in, "Oversized field name length", false);
            currentPayload += fieldLength;
            if (currentPayload > maxPayloadSize) {
                throw new InvalidFrameProtocolException("Oversized payload: " + currentPayload);
            }
            ByteBuf fieldBuf = in.readSlice(fieldLength);
            String field = fieldBuf.toString(StandardCharsets.UTF_8);

            int dataLength = tryReadUnsigned(in, "Oversized field data length", true);
            currentPayload += dataLength;
            if (currentPayload > maxPayloadSize) {
                throw new InvalidFrameProtocolException("Oversized payload: " + currentPayload);
            }
            ByteBuf dataBuf = in.readSlice(dataLength);
            String data = dataBuf.toString(StandardCharsets.UTF_8);

            dataMap.put(field, data);
        }
        Message message = new Message(sequence, dataMap);
        ((V1Batch) batch).addMessage(message);

        if (batch.isComplete()) {
            batchComplete(out);
        }
        transition(States.READ_HEADER);
    }

    private void decodeJsonHeader(ByteBuf in) throws InvalidFrameProtocolException {
        sequence = tryReadUnsigned(in, "Invalid sequence number", true);
        int jsonPayloadSize = tryReadUnsigned(in, "Invalid json length", false);
        transition(States.READ_JSON, jsonPayloadSize);
    }

    private void decodeCompressedFrameHeader(ByteBuf in) throws InvalidFrameProtocolException {
        int compressedFrameSize = tryReadUnsigned(in, "Invalid compressed frame size", false);
        transition(States.READ_COMPRESSED_FRAME, compressedFrameSize);
    }

    private void decodeJson(ByteBuf in, List<Object> out) throws InvalidFrameProtocolException {
        ((V2Batch) batch).addMessage(sequence, in, requiredBytes);
        if (batch.isComplete()) {
            batchComplete(out);
        }
        transition(States.READ_HEADER);
    }

    private void inflateCompressedFrame(ChannelHandlerContext ctx, ByteBuf in, List<Object> content)
            throws IOException, InvalidFrameProtocolException {
        ByteBuffer buffer = in.nioBuffer();
        // Estimation of decompressed out. It's a json body, a good compression ratio can be expected
        ByteBuf expandedPayload = ctx.alloc().buffer(requiredBytes * 8, Math.max(requiredBytes * 8, maxPayloadSize));
        try {
            inflater.setInput(buffer);
            // Temporary buffer for decompression
            byte[] tmp = new byte[8192];
            do {
                int len = inflater.inflate(tmp);
                if (len > 0) {
                    if ((expandedPayload.readableBytes() + len) > maxPayloadSize) {
                        throw new InvalidFrameProtocolException("Oversized compressed payload: " + (expandedPayload.readableBytes() + len));
                    }
                    expandedPayload.writeBytes(tmp, 0, len);
                }
            } while ( ! inflater.finished() && ! inflater.needsInput());
            in.skipBytes(buffer.position());
            walkCompressedPayload(ctx, expandedPayload, content);
        } catch (DataFormatException e) {
            throw new IOException("Invalid compressed data", e);
        } finally {
            expandedPayload.release();
            inflater.end();
        }
    }

    private void walkCompressedPayload(ChannelHandlerContext ctx, ByteBuf in, List<Object> content)
            throws InvalidFrameProtocolException, IOException {
        transition(States.READ_HEADER);

        decodingCompressedBuffer = true;
        try {
            while (in.readableBytes() > 0) {
                decode(ctx, in, content);
            }
        } finally {
            decodingCompressedBuffer = false;
            transition(States.READ_HEADER);
        }
    }

    private boolean hasEnoughBytes(ByteBuf in) {
        return in.readableBytes() >= requiredBytes;
    }

    private void transition(States next) {
        transition(next, next.length);
    }

    private void transition(States next, int requiredBytes) {
        logger.trace("{}", () -> "Transition, from: " + currentState + ", to: " + next + ", requiring " + requiredBytes + " bytes");
        this.currentState = next;
        this.requiredBytes = requiredBytes;
    }

    private void batchComplete(List<Object> out) {
        logger.trace("{}", () -> "Sending batch size: " + batch.size() + ", windowSize: " + batch.getBatchSize() + " , seq: " + sequence);
        requiredBytes = 0;
        sequence = 0;
        out.add(batch);
        batch = null;
    }
    
    private void resetOnError(ByteBuf in, List<Object> out) {
        in.clear();
        currentState = States.READ_HEADER;
        requiredBytes = States.READ_HEADER.length;
        if (batch != null) {
            out.add(batch);
            batch = null;
        }
    }

    private int tryReadUnsigned(ByteBuf in, String message, boolean canZero) throws InvalidFrameProtocolException {
        long trylong = in.readUnsignedInt();
        if (trylong == 0 && ! canZero) {
            throw new InvalidFrameProtocolException(message + ", received: " + trylong);
        }
        try {
            return Math.toIntExact(trylong);
        } catch (ArithmeticException e) {
            throw new InvalidFrameProtocolException(message + ", received: " + trylong);
        }
    }

    public static class InvalidFrameProtocolException extends Exception {
        InvalidFrameProtocolException(String message) {
            super(message);
        }
    }

}

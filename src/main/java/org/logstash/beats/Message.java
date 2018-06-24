package org.logstash.beats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

public class Message implements Comparable<Message> {

    private final int sequence;
    private String identityStream;
    private Map<?, ?> data;
    private Batch batch;
    private ByteBuf buffer;

    private static final JsonFactory factory = new JsonFactory();
    public static final ThreadLocal<ObjectMapper> MAPPER = ThreadLocal.withInitial(() -> new ObjectMapper(factory).registerModule(new AfterburnerModule()));

    /**
     * Create a message using a map of key, value pairs
     * @param sequence sequence number of the message
     * @param map key/value pairs representing the message
     */
    public Message(int sequence, Map<?, ?> map) {
        this.sequence = sequence;
        this.data = map;
    }

    /**
     * Create a message using a ByteBuf holding a Json object.
     * Note that this ctr is *lazy* - it will not deserialize the Json object until it is needed.
     * @param sequence sequence number of the message
     * @param buffer {@link ByteBuf} buffer containing Json object
     */
    public Message(int sequence, ByteBuf buffer) {
        this.sequence = sequence;
        this.buffer = buffer;
    }

    /**
     * Returns the sequence number of this messsage
     * @return
     */
    public int getSequence() {
        return sequence;
    }

    /**
     * Returns a list of key/value pairs representing the contents of the message.
     * Note that this method is lazy if the Message was created using a {@link ByteBuf}
     * @return {@link Map} Map of key/value pairs
     */
    public Map<?, ?> getData() {
        if (data == null && buffer != null) {
            try (InputStream byteBufInputStream = new ByteBufInputStream(buffer)) {
                data = MAPPER.get().readValue(byteBufInputStream, Map.class);
                buffer = null;
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to parse beats payload ", e);
            }
        }
        return data;
    }

    @Override
    public int compareTo(Message o) {
        return Integer.compare(getSequence(), o.getSequence());
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }


    public String getIdentityStream() {
        if (identityStream == null) {
            identityStream = extractIdentityStream();
        }
        return identityStream;
    }

    private String extractIdentityStream() {
        @SuppressWarnings("unchecked")
        Map<String, String> beatsData = (Map<String, String>) getData().get("beat");

        if (beatsData != null) {
            String id = beatsData.get("id");
            String resourceId = beatsData.get("resource_id");

            if (id != null && resourceId != null) {
                return id + "-" + resourceId;
            } else {
                return beatsData.get("name") + "-" + beatsData.get("source");
            }
        }

        return null;
    }

}

package org.logstash.beats;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class MessageTest {

    @Test
    void testGetData() {
        Map<String, Object> map = new HashMap<>();

        Message message = new Message(1, map);
        assertEquals(map, message.getData());
    }

    @Test
    void testGetSequence() {
        Map<String, Object> map = new HashMap<>();

        Message message = new Message(1, map);
        assertEquals(1, message.getSequence());
    }

    @Test
    void testComparison() {
        Map<String, Object> map = new HashMap<>();
        Message messageOlder = new Message(1, map);
        Message messageNewer = new Message(2, map);

        assertTrue(messageNewer.getSequence() > messageOlder.getSequence());
    }

    @Test
    void tesGenerateAnIdentityStreamWhenIdAndResourceArePresent() {
        Map<String, Object> map = new HashMap<>();
        Map<String, String> beatsData = new HashMap<>();

        beatsData.put("id", "uuid1234");
        beatsData.put("resource_id", "rid1234");

        map.put("beat", beatsData);
        Message message = new Message(1, map);

        assertEquals("uuid1234-rid1234", message.getIdentityStream());
    }

    @Test
    void tesGenerateAnIdentityStreamWhenResourceIdIsAbsent() {
        Map<String, Object> map = new HashMap<>();
        Map<String, String> beatsData = new HashMap<>();

        beatsData.put("id", "uuid1234");
        beatsData.put("name", "filebeat");
        beatsData.put("source", "/var/log/message.log");

        map.put("beat", beatsData);
        Message message = new Message(1, map);

        assertEquals("filebeat-/var/log/message.log", message.getIdentityStream());
    }


    @Test
    void tesGenerateAnIdentityStreamWhenIdIsAbsent() {
        Map<String, Object> map = new HashMap<>();
        Map<String, String> beatsData = new HashMap<>();

        beatsData.put("resource_id", "rid1234");
        beatsData.put("name", "filebeat");
        beatsData.put("source", "/var/log/message.log");

        map.put("beat", beatsData);
        Message message = new Message(1, map);

        assertEquals("filebeat-/var/log/message.log", message.getIdentityStream());
    }

    @Test
    void tesGenerateAnIdentityStreamWhenIdAndResourceIdAreAbsent() {
        Map<String, Object> map = new HashMap<>();

        Message message = new Message(1, map);

        assertNull(message.getIdentityStream());
    }

}

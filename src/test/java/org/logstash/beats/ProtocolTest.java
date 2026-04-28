package org.logstash.beats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by ph on 2016-06-01.
 */
class ProtocolTest {

    @Test
    void isVersion2Test() {
        assertTrue(Protocol.isVersion2((byte) '2'));
        assertFalse(Protocol.isVersion2((byte) '1'));
    }

}

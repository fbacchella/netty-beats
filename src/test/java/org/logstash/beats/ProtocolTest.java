package org.logstash.beats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by ph on 2016-06-01.
 */
public class ProtocolTest {

    @Test
    public void isVersion2Test() {
        assertTrue(Protocol.isVersion2((byte) '2'));
        assertFalse(Protocol.isVersion2((byte) '1'));
    }

}

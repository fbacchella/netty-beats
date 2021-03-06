package org.logstash.beats;

/**
 * Created by ph on 2016-05-16.
 */
public class Protocol {

    public static final byte VERSION_1 = '1';
    public static final byte VERSION_2 = '2';

    public static final byte CODE_WINDOW_SIZE = 'W';
    public static final byte CODE_JSON_FRAME = 'J';
    public static final byte CODE_COMPRESSED_FRAME = 'C';
    public static final byte CODE_FRAME = 'D';
    public static final byte CODE_ACK = 'A';

    private Protocol() {

    }

    public static boolean isVersion2(byte versionRead) {
        return Protocol.VERSION_2 == versionRead;
    }

    public static boolean isVersion1(byte versionRead) {
        return Protocol.VERSION_1 == versionRead;
    }

}

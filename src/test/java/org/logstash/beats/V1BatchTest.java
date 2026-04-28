package org.logstash.beats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class V1BatchTest {

    private V1Batch batch;

    @BeforeEach
    void setUp() {
        batch = new V1Batch();
    }

    @Test
    void testIsEmpty() {
        assertTrue(batch.isEmpty());
        batch.addMessage(new Message(1, new HashMap<>()));
        assertFalse(batch.isEmpty());
    }

    @Test
    void testSize() {
        assertEquals(0, batch.size());
        batch.addMessage(new Message(1, new HashMap<>()));
        assertEquals(1, batch.size());
    }

    @Test
    void testGetProtocol() {
        assertEquals(Protocol.VERSION_1, batch.getProtocol());
    }

    @Test
    void testCompleteReturnTrueWhenIReceiveTheSameAmountOfEvent() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message(i, new HashMap<>()));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    void testCompleteBatchWithSequenceNumbersNotStartingAtOne() {
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message(startSequenceNumber + i, Collections.emptyMap()));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    void testHighSequence(){
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message(startSequenceNumber + i, Collections.emptyMap()));
        }

        assertEquals(startSequenceNumber + numberOfEvent, batch.getHighestSequence());
    }

    @Test
    void TestCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        batch.addMessage(new Message(1, new HashMap<>()));

        assertFalse(batch.isComplete());
    }

}

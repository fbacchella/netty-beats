package org.logstash.beats;

public record Ack(byte protocol, int sequence) {
}

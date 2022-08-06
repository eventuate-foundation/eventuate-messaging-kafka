package io.eventuate.messaging.kafka.consumer;

public class RawKafkaMessage {
  private final String messageKey;
  private final byte[] payload;

  public RawKafkaMessage(String messageKey, byte[] payload) {
    this.messageKey = messageKey;
    this.payload = payload;
  }

  public String getMessageKey() {
    return messageKey;
  }

  public byte[] getPayload() {
    return payload;
  }
}

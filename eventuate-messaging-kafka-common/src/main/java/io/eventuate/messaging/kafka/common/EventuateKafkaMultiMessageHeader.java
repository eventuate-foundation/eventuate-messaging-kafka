package io.eventuate.messaging.kafka.common;

public class EventuateKafkaMultiMessageHeader extends KeyValue {

  public EventuateKafkaMultiMessageHeader(String key, String value) {
    super(key, value);
  }
}

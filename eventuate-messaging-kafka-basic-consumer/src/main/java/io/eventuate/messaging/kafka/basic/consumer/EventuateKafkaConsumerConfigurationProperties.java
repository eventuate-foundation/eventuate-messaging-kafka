package io.eventuate.messaging.kafka.basic.consumer;

import java.util.HashMap;
import java.util.Map;

public class EventuateKafkaConsumerConfigurationProperties {
  Map<String, String> properties = new HashMap<>();

  public Map<String, String> getProperties() {
    return properties;
  }

  public EventuateKafkaConsumerConfigurationProperties() {
  }

  public EventuateKafkaConsumerConfigurationProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public static EventuateKafkaConsumerConfigurationProperties empty() {
    return new EventuateKafkaConsumerConfigurationProperties();
  }
}

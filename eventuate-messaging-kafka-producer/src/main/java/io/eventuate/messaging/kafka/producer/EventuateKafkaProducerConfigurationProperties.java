package io.eventuate.messaging.kafka.producer;

import java.util.HashMap;
import java.util.Map;

public class EventuateKafkaProducerConfigurationProperties {
  Map<String, String> properties = new HashMap<>();

  public EventuateKafkaProducerConfigurationProperties() {
  }

  public EventuateKafkaProducerConfigurationProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public static EventuateKafkaProducerConfigurationProperties empty() {
    return new EventuateKafkaProducerConfigurationProperties();
  }
}

package io.eventuate.messaging.kafka.basic.consumer.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties("eventuate.local.kafka.consumer")
public class EventuateKafkaConsumerSpringConfigurationProperties {
  Map<String, String> properties = new HashMap<>();

  public Map<String, String> getProperties() {
    return properties;
  }
}

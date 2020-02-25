package io.eventuate.messaging.kafka.micronaut.producer;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@ConfigurationProperties("eventuate.local.kafka.producer")
public class EventuateKafkaProducerMicronautConfigurationProperties {
  Map<String, String> properties = new HashMap<>();

  public Map<String, String> getProperties() {
    return properties
            .entrySet()
            .stream()
            .collect(Collectors.toMap(o -> o.getKey().replace("-", "."), Map.Entry::getValue));
  }
}

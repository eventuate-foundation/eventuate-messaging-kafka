package io.eventuate.messaging.kafka.basic.consumer.micronaut;

import io.micronaut.context.annotation.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@ConfigurationProperties("eventuate.local.kafka.consumer")
public class EventuateKafkaConsumerMicronautConfigurationProperties {
  Map<String, String> properties = new HashMap<>();

  public Map<String, String> getProperties() {
    return properties
            .entrySet()
            .stream()
            .collect(Collectors.toMap(o -> o.getKey().replace("-", "."), Map.Entry::getValue));
  }
}

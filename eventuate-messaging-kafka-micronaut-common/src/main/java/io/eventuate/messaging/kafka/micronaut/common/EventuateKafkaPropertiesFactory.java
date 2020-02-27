package io.eventuate.messaging.kafka.micronaut.common;

import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;

@Factory
public class EventuateKafkaPropertiesFactory {

  @Bean
  public EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties(@Value("${eventuatelocal.kafka.bootstrap.servers}")
                                                                                               String bootstrapServers,
                                                                                     @Value("${eventuatelocal.kafka.connection.validation.timeout:1000}")
                                                                                             long connectionValidationTimeout) {
    return new EventuateKafkaConfigurationProperties(bootstrapServers, connectionValidationTimeout);
  }
}

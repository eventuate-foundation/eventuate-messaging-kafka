package io.eventuate.messaging.kafka.micronaut.producer;

import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.micronaut.context.annotation.Factory;

import javax.inject.Singleton;

@Factory
public class EventuateKafkaProducerConfigurationPropertiesFactory {
  @Singleton
  public EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties(EventuateKafkaProducerMicronautConfigurationProperties eventuateKafkaProducerMicronautConfigurationProperties) {
    return new EventuateKafkaProducerConfigurationProperties(eventuateKafkaProducerMicronautConfigurationProperties.getProperties());
  }
}

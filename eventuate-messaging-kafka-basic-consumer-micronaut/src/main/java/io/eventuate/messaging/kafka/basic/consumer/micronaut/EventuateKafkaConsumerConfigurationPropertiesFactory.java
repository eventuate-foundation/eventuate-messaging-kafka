package io.eventuate.messaging.kafka.basic.consumer.micronaut;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Factory;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

@Factory
public class EventuateKafkaConsumerConfigurationPropertiesFactory {
  @Singleton
  public EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties(EventuateKafkaConsumerMicronautConfigurationProperties eventuateKafkaConsumerMicronautConfigurationProperties) {
    return new EventuateKafkaConsumerConfigurationProperties(eventuateKafkaConsumerMicronautConfigurationProperties.getProperties());
  }
}

package io.eventuate.messaging.kafka.micronaut.consumer;

import javax.inject.Singleton;

import io.eventuate.messaging.kafka.basic.consumer.DefaultKafkaConsumerFactory;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.micronaut.context.annotation.Factory;

@Factory
public class KafkaConsumerFactoryFactory {
  @Singleton
  KafkaConsumerFactory kafkaConsumerFactory() {
    return new DefaultKafkaConsumerFactory();
  }
}

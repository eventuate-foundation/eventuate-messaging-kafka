package io.eventuate.messaging.kafka.micronaut.consumer;

import javax.inject.Singleton;

import io.eventuate.messaging.kafka.basic.consumer.DefaultKafkaConsumerFactory;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;

@Factory
public class KafkaConsumerFactoryFactory {
  @Singleton
  @Requires(missingBeans = KafkaConsumerFactory.class)
  public KafkaConsumerFactory kafkaConsumerFactory() {
    return new DefaultKafkaConsumerFactory();
  }
}

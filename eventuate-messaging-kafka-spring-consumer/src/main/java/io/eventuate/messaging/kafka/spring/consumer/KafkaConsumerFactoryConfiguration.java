package io.eventuate.messaging.kafka.spring.consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import io.eventuate.messaging.kafka.basic.consumer.DefaultKafkaConsumerFactory;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;

@Configuration
public class KafkaConsumerFactoryConfiguration {
  @Bean
  KafkaConsumerFactory kafkaConsumerFactory() {
    return new DefaultKafkaConsumerFactory();
  }
}

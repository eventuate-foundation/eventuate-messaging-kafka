package io.eventuate.messaging.kafka.spring.common;

import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventuateKafkaPropertiesConfiguration {



  @Bean
  public EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties(@Value("${eventuatelocal.kafka.bootstrap.servers}")
                                                                                               String bootstrapServers,
                                                                                     @Value("${eventuatelocal.kafka.connection.validation.timeout:#{1000}}")
                                                                                             long connectionValidationTimeout) {
    return new EventuateKafkaConfigurationProperties(bootstrapServers, connectionValidationTimeout);
  }
}

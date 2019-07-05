package io.eventuate.messaging.kafka.basic.consumer.spring;

import io.eventuate.messaging.kafka.basic.consumer.AbstractEventuateKafkaBasicConsumerTest;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.common.spring.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.producer.spring.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EventuateKafkaBasicConsumerSpringTest.EventuateKafkaConsumerTestConfiguration.class)
public class EventuateKafkaBasicConsumerSpringTest extends AbstractEventuateKafkaBasicConsumerTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaPropertiesConfiguration.class})
  public static class EventuateKafkaConsumerTestConfiguration {
    @Bean
    public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties, EventuateKafkaProducerConfigurationProperties producerProperties) {
      return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
    }
  }

  @Autowired
  private EventuateKafkaConfigurationProperties kafkaProperties;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties consumerProperties;

  @Autowired
  private EventuateKafkaProducer producer;

  @Test
  @Override
  public void shouldStopWhenHandlerThrowsException() {
    super.shouldStopWhenHandlerThrowsException();
  }

  @Override
  protected EventuateKafkaConfigurationProperties getKafkaProperties() {
    return kafkaProperties;
  }

  @Override
  protected EventuateKafkaConsumerConfigurationProperties getConsumerProperties() {
    return consumerProperties;
  }

  @Override
  protected EventuateKafkaProducer getProducer() {
    return producer;
  }
}
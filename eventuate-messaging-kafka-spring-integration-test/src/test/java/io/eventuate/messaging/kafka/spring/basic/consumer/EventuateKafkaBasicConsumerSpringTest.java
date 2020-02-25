package io.eventuate.messaging.kafka.spring.basic.consumer;

import io.eventuate.messaging.kafka.basic.consumer.AbstractEventuateKafkaBasicConsumerTest;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
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
@SpringBootTest(classes = EventuateKafkaBasicConsumerSpringTest.EventuateKafkaConsumerTestConfiguration.class,
        properties = "eventuate.local.kafka.consumer.backPressure.high=3")
public class EventuateKafkaBasicConsumerSpringTest extends AbstractEventuateKafkaBasicConsumerTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaPropertiesConfiguration.class})
  public static class EventuateKafkaConsumerTestConfiguration {
    @Bean
    public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties,
                                           EventuateKafkaProducerConfigurationProperties producerProperties) {
      return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
    }

    @Bean
    public MessageConsumerKafkaImpl messageConsumerKafka(EventuateKafkaConfigurationProperties props,
                                                         EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties) {
      return new MessageConsumerKafkaImpl(props.getBootstrapServers(), eventuateKafkaConsumerConfigurationProperties);
    }
  }

  @Autowired
  private EventuateKafkaConfigurationProperties kafkaProperties;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties consumerProperties;

  @Autowired
  private EventuateKafkaProducer producer;

  @Autowired
  private MessageConsumerKafkaImpl consumer;

  @Test
  @Override
  public void shouldStopWhenHandlerThrowsException() {
    super.shouldStopWhenHandlerThrowsException();
  }

  @Test
  @Override
  public void shouldConsumeMessages() {
    super.shouldConsumeMessages();
  }

  @Test
  @Override
  public void shouldConsumeMessagesWithBackPressure() {
    super.shouldConsumeMessagesWithBackPressure();
  }

  @Test
  @Override
  public void shouldConsumeBatchOfMessage() {
    super.shouldConsumeBatchOfMessage();
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

  @Override
  protected MessageConsumerKafkaImpl getConsumer() {
    return consumer;
  }
}
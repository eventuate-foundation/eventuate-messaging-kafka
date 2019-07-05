package io.eventuate.messaging.kafka.basic.consumer;

import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

@MicronautTest
@Property(name = "micronaut.eventuate.kafka.properties.factory", value = "true")
public class EventuateKafkaBasicConsumerMicronautTest extends AbstractEventuateKafkaBasicConsumerTest {

  @Inject
  private EventuateKafkaConfigurationProperties kafkaProperties;

  @Inject
  private EventuateKafkaConsumerConfigurationProperties consumerProperties;

  @Inject
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
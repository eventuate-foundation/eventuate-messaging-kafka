package io.eventuate.messaging.kafka.spring.basic.consumer;

import io.eventuate.messaging.kafka.basic.consumer.AbstractEventuateKafkaBasicConsumerTest;
import io.eventuate.messaging.kafka.basic.consumer.DefaultKafkaConsumerFactory;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.*;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = EventuateKafkaBasicConsumerSpringTest.EventuateKafkaConsumerTestConfiguration.class,
        properties = "eventuate.local.kafka.consumer.backPressure.high=3")
public class EventuateKafkaBasicConsumerSpringTest extends AbstractEventuateKafkaBasicConsumerTest {

  @Configuration
  @EnableAutoConfiguration
  @Import({EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaPropertiesConfiguration.class})
  public static class EventuateKafkaConsumerTestConfiguration {

    @Autowired(required=false)
    private TopicPartitionToSwimlaneMapping partitionToSwimLaneMapping = new OriginalTopicPartitionToSwimlaneMapping();

    @Bean
    public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties,
                                           EventuateKafkaProducerConfigurationProperties producerProperties) {
      return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
    }

    @Bean
    public MessageConsumerKafkaImpl messageConsumerKafka(EventuateKafkaConfigurationProperties props,
                                                         EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                         KafkaConsumerFactory kafkaConsumerFactory) {
      return new MessageConsumerKafkaImpl(props.getBootstrapServers(), eventuateKafkaConsumerConfigurationProperties, kafkaConsumerFactory, partitionToSwimLaneMapping);
    }

    @Bean
    public KafkaConsumerFactory kafkaConsumerFactory() {
      return new DefaultKafkaConsumerFactory();
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

  @Autowired
  private KafkaConsumerFactory kafkaConsumerFactory;

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

  @Override
  protected KafkaConsumerFactory getKafkaConsumerFactory() {
    return kafkaConsumerFactory;
  }

  @Test
  public void shouldSaveOffsets() throws InterruptedException {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();
    LinkedBlockingQueue<KafkaMessage> messages = new LinkedBlockingQueue<>();

    sendMessages(topic);

    handler = kafkaMessage -> {
      try {
        TimeUnit.MILLISECONDS.sleep(20);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      messages.add(kafkaMessage);
    };

    KafkaSubscription subscription = getConsumer().subscribe(subscriberId, Collections.singleton(topic), handler);

    Eventually.eventually(() ->
      assertEquals(2, messages.size()));

    subscription.close();

    messages.clear();

    subscription = getConsumer().subscribe(subscriberId, Collections.singleton(topic), handler);
    TimeUnit.SECONDS.sleep(10);

    assertThat(messages).isEmpty();

  }

}
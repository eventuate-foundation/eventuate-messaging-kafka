package io.eventuate.messaging.kafka.consumer;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerMessageHandler;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessage;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.util.test.async.Eventually;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = MessageConsumerKafkaImplTest.Config.class,
        properties = "eventuate.local.kafka.consumer.backPressure.high=3")
public class MessageConsumerKafkaImplTest {

  @Configuration
  @EnableAutoConfiguration
  @EnableConfigurationProperties({EventuateKafkaConsumerConfigurationProperties.class, EventuateKafkaProducerConfigurationProperties.class})
  @Import(MessageConsumerKafkaConfiguration.class)
  public static class Config {

    @Bean
    public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties, EventuateKafkaProducerConfigurationProperties producerProperties) {
      return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
    }


  }
  private KafkaMessageHandler handler;

  @Autowired
  private EventuateKafkaConfigurationProperties kafkaProperties;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties consumerProperties;

  @Autowired
  private EventuateKafkaProducer producer;

  @Autowired
  private MessageConsumerKafkaImpl consumer;


  @Test
  public void shouldConsumeMessages() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();

    sendMessages(topic);

    handler = mock(KafkaMessageHandler.class);

    KafkaSubscription subscription = consumer.subscribe(subscriberId, Collections.singleton(topic), handler);

    Eventually.eventually(() -> {
      verify(handler, atLeastOnce()).accept(any());
    });

    subscription.close();
  }

  @Test
  public void shouldConsumeBatchOfMessage() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();

    List<EventuateKafkaMultiMessage> messages = Arrays.asList(new EventuateKafkaMultiMessage(null, "a"),
            new EventuateKafkaMultiMessage(null, "b"), new EventuateKafkaMultiMessage(null, "c"));

    producer.send(topic, null, new EventuateKafkaMultiMessageConverter().convertMessagesToBytes(messages));

    handler = mock(KafkaMessageHandler.class);

    KafkaSubscription subscription = consumer.subscribe(subscriberId, Collections.singleton(topic), handler);

    Eventually.eventually(() -> {
      verify(handler, times(3)).accept(any());
    });

    subscription.close();
  }

  private EventuateKafkaConsumer makeConsumer(String subscriberId, String topic, EventuateKafkaConsumerMessageHandler handler) {
    EventuateKafkaConsumer consumer = new EventuateKafkaConsumer(subscriberId, handler, Collections.singletonList(topic), kafkaProperties.getBootstrapServers(), consumerProperties);

    consumer.start();
    return consumer;
  }

  private void sendMessages(String topic) {
    producer.send(topic, null, "a");
    producer.send(topic, null, "b");
  }

  @Test
  public void shouldConsumeMessagesWithBackPressure() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();
    LinkedBlockingQueue<KafkaMessage> messages = new LinkedBlockingQueue<>();

    for (int i = 0 ; i < 100; i++)
      sendMessages(topic);

    handler = new KafkaMessageHandler() {
      @Override
      public void accept(KafkaMessage kafkaMessage) {
        try {
          TimeUnit.MILLISECONDS.sleep(20);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        messages.add(kafkaMessage);
      }
    };

    KafkaSubscription subscription = consumer.subscribe(subscriberId, Collections.singleton(topic), handler);

    Eventually.eventually(() -> {
      assertEquals(200, messages.size());
    });

    subscription.close();

  }

}
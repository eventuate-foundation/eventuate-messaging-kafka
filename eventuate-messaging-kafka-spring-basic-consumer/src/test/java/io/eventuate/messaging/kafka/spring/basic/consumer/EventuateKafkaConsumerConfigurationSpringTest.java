package io.eventuate.messaging.kafka.spring.basic.consumer;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class)
public class EventuateKafkaConsumerConfigurationSpringTest {

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  @Test
  public void testPropertyParsing() {

    assertEquals(2, eventuateKafkaConsumerConfigurationProperties.getProperties().size());

    assertEquals("10000", eventuateKafkaConsumerConfigurationProperties.getProperties().get("session.timeout.ms"));

    assertEquals("org.apache.kafka.common.serialization.StringSerializer",
            eventuateKafkaConsumerConfigurationProperties.getProperties().get("key.serializer"));

    assertEquals(5, eventuateKafkaConsumerConfigurationProperties.getBackPressure().getLow());
    assertEquals(100, eventuateKafkaConsumerConfigurationProperties.getBackPressure().getHigh());
    assertEquals(200, eventuateKafkaConsumerConfigurationProperties.getPollTimeout());
  }
}

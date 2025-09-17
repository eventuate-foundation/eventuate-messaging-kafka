package io.eventuate.messaging.kafka.spring.producer;

import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class)
public class EventuateKafkaProducerConfigurationSpringTest {

  @Autowired
  private EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties;

  @Test
  public void testPropertyParsing() {

    Assertions.assertEquals(2, eventuateKafkaProducerConfigurationProperties.getProperties().size());

    Assertions.assertEquals("1000000", eventuateKafkaProducerConfigurationProperties.getProperties().get("buffer.memory"));

    Assertions.assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer",
            eventuateKafkaProducerConfigurationProperties.getProperties().get("value.serializer"));
  }
}

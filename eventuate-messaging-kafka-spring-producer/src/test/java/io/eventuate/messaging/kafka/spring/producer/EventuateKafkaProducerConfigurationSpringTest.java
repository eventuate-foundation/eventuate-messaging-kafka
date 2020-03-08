package io.eventuate.messaging.kafka.spring.producer;

import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class)
public class EventuateKafkaProducerConfigurationSpringTest {

  @Autowired
  private EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties;

  @Test
  public void testPropertyParsing() {

    Assert.assertEquals(2, eventuateKafkaProducerConfigurationProperties.getProperties().size());

    Assert.assertEquals("1000000", eventuateKafkaProducerConfigurationProperties.getProperties().get("buffer.memory"));

    Assert.assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer",
            eventuateKafkaProducerConfigurationProperties.getProperties().get("value.serializer"));
  }
}

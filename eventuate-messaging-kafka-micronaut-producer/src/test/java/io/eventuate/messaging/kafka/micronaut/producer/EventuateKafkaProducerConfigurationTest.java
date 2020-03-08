package io.eventuate.messaging.kafka.micronaut.producer;

import io.micronaut.test.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

@MicronautTest(propertySources = "application.properties")
public class EventuateKafkaProducerConfigurationTest {

  @Inject
  private EventuateKafkaProducerMicronautConfigurationProperties eventuateKafkaProducerConfigurationProperties;

  @Test
  public void testPropertyParsing() {

    Assert.assertEquals(2, eventuateKafkaProducerConfigurationProperties.getProperties().size());

    Assert.assertEquals("1000000", eventuateKafkaProducerConfigurationProperties.getProperties().get("buffer.memory"));

    Assert.assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer",
            eventuateKafkaProducerConfigurationProperties.getProperties().get("value.serializer"));
  }
}

package io.eventuate.messaging.kafka.basic.consumer.micronaut;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

@MicronautTest(propertySources = "application.properties")
public class EventuateKafkaConsumerConfigurationMicronautTest {

  @Inject
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  @Test
  public void testPropertyParsing() {

    Assert.assertEquals(2, eventuateKafkaConsumerConfigurationProperties.getProperties().size());

    Assert.assertEquals("10000", eventuateKafkaConsumerConfigurationProperties.getProperties().get("session.timeout.ms"));

    Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer",
            eventuateKafkaConsumerConfigurationProperties.getProperties().get("key.serializer"));
  }
}

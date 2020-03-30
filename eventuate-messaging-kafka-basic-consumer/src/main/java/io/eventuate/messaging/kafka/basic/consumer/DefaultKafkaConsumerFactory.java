package io.eventuate.messaging.kafka.basic.consumer;

import java.util.Properties;

public class DefaultKafkaConsumerFactory implements KafkaConsumerFactory {

  @Override
  public KafkaMessageConsumer makeConsumer(String subscriptionId, Properties consumerProperties) {
    return DefaultKafkaMessageConsumer.create(consumerProperties);
  }
}

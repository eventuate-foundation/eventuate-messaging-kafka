package io.eventuate.messaging.kafka.basic.consumer;

import java.util.Properties;

public interface KafkaConsumerFactory {

  KafkaMessageConsumer makeConsumer(String subscriptionId, Properties consumerProperties);

}

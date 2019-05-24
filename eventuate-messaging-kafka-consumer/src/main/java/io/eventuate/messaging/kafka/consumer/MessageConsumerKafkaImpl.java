package io.eventuate.messaging.kafka.consumer;

import io.eventuate.common.messaging.CommonMessageConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class MessageConsumerKafkaImpl implements CommonMessageConsumer {

  private Logger logger = LoggerFactory.getLogger(getClass());

  private final String id = UUID.randomUUID().toString();

  private String bootstrapServers;
  private List<EventuateKafkaConsumer> consumers = new ArrayList<>();
  public MessageConsumerKafkaImpl(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  public KafkaSubscription subscribe(String subscriberId, Set<String> channels, KafkaMessageHandler handler) {

    SwimlaneBasedDispatcher swimlaneBasedDispatcher = new SwimlaneBasedDispatcher(subscriberId, Executors.newCachedThreadPool());

    EventuateKafkaConsumerMessageHandler kcHandler = (record, callback) -> {
      swimlaneBasedDispatcher.dispatch(new KafkaMessage(record.value()),
              record.partition(),
              message -> handle(message, callback, subscriberId, handler));
    };

    EventuateKafkaConsumer kc = new EventuateKafkaConsumer(subscriberId,
            kcHandler,
            new ArrayList<>(channels),
            bootstrapServers,
            eventuateKafkaConsumerConfigurationProperties);

    consumers.add(kc);

    kc.start();

    return new KafkaSubscription(() -> {
      kc.stop();
      consumers.remove(kc);
    });
  }

  public void handle(KafkaMessage message, BiConsumer<Void, Throwable> callback, String subscriberId, KafkaMessageHandler kafkaMessageHandler) {
    try {
      kafkaMessageHandler.accept(message);
      callback.accept(null, null);
    } catch (Throwable e) {
      callback.accept(null, e);
      throw e;
    }
  }

  @Override
  public void close() {
    consumers.forEach(EventuateKafkaConsumer::stop);
  }

  public String getId() {
    return id;
  }
}

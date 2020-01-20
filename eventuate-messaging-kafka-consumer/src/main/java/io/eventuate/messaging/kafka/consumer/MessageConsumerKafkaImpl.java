package io.eventuate.messaging.kafka.consumer;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerMessageHandler;
import io.eventuate.messaging.kafka.common.EventuateBinaryMessageEncoding;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessage;
import io.eventuate.messaging.partitionmanagement.CommonMessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class MessageConsumerKafkaImpl implements CommonMessageConsumer {

  private Logger logger = LoggerFactory.getLogger(getClass());

  private final String id = UUID.randomUUID().toString();

  private String bootstrapServers;
  private List<EventuateKafkaConsumer> consumers = new ArrayList<>();
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;
  private EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

  public MessageConsumerKafkaImpl(String bootstrapServers,
                                  EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties) {
    this.bootstrapServers = bootstrapServers;
    this.eventuateKafkaConsumerConfigurationProperties = eventuateKafkaConsumerConfigurationProperties;
  }


  public KafkaSubscription subscribe(String subscriberId, Set<String> channels, KafkaMessageHandler handler) {

    SwimlaneBasedDispatcher swimlaneBasedDispatcher = new SwimlaneBasedDispatcher(subscriberId, Executors.newCachedThreadPool());

    EventuateKafkaConsumerMessageHandler kcHandler = (record, callback) -> swimlaneBasedDispatcher.dispatch(new RawKafkaMessage(record.value()),
            record.partition(),
            message -> handle(message, callback, handler));

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

  public void handle(RawKafkaMessage message, BiConsumer<Void, Throwable> callback, KafkaMessageHandler kafkaMessageHandler) {
    try {
      if (eventuateKafkaMultiMessageConverter.isMultiMessage(message.getPayload())) {
        eventuateKafkaMultiMessageConverter
                .convertBytesToMessages(message.getPayload())
                .getMessages()
                .stream()
                .map(EventuateKafkaMultiMessage::getValue)
                .map(KafkaMessage::new)
                .forEach(kafkaMessageHandler);
      }
      else {
        kafkaMessageHandler.accept(new KafkaMessage(EventuateBinaryMessageEncoding.bytesToString(message.getPayload())));
      }
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

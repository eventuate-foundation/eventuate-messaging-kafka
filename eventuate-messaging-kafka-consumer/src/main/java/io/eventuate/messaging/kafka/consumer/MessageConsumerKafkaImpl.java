package io.eventuate.messaging.kafka.consumer;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumer;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerMessageHandler;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateBinaryMessageEncoding;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessage;
import io.eventuate.messaging.partitionmanagement.CommonMessageConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class MessageConsumerKafkaImpl implements CommonMessageConsumer {

  private Logger logger = LoggerFactory.getLogger(getClass());

  private final String id = UUID.randomUUID().toString();

  private String bootstrapServers;
  private List<EventuateKafkaConsumer> consumers = new ArrayList<>();
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;
  private KafkaConsumerFactory kafkaConsumerFactory;
  private EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

  public MessageConsumerKafkaImpl(String bootstrapServers,
                                  EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                  KafkaConsumerFactory kafkaConsumerFactory) {
    this.bootstrapServers = bootstrapServers;
    this.eventuateKafkaConsumerConfigurationProperties = eventuateKafkaConsumerConfigurationProperties;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
  }

  public KafkaSubscription subscribe(String subscriberId, Set<String> channels, KafkaMessageHandler handler) {
    return subscribeWithReactiveHandler(subscriberId, channels, kafkaMessage -> {
      handler.accept(kafkaMessage);
      return CompletableFuture.completedFuture(null);
    });
  }

  public KafkaSubscription subscribeWithReactiveHandler(String subscriberId, Set<String> channels, ReactiveKafkaMessageHandler handler) {

    SwimlaneBasedDispatcher swimlaneBasedDispatcher = new SwimlaneBasedDispatcher(subscriberId, Executors.newCachedThreadPool());

    EventuateKafkaConsumerMessageHandler kcHandler = (record, callback) -> {
      if (eventuateKafkaMultiMessageConverter.isMultiMessage(record.value())) {
        return handleBatch(record, swimlaneBasedDispatcher, callback, handler);
      } else {
        return swimlaneBasedDispatcher.dispatch(new RawKafkaMessage(record.value()), record.partition(), message -> handle(message, callback, handler));
      }
    };

    EventuateKafkaConsumer kc = new EventuateKafkaConsumer(subscriberId,
            kcHandler,
            new ArrayList<>(channels),
            bootstrapServers,
            eventuateKafkaConsumerConfigurationProperties,
            kafkaConsumerFactory);

    consumers.add(kc);

    kc.start();

    return new KafkaSubscription(() -> {
      kc.stop();
      consumers.remove(kc);
    });
  }

  private SwimlaneDispatcherBacklog handleBatch(ConsumerRecord<String, byte[]> record,
                                                SwimlaneBasedDispatcher swimlaneBasedDispatcher,
                                                BiConsumer<Void, Throwable> callback,
                                                ReactiveKafkaMessageHandler handler) {
    return eventuateKafkaMultiMessageConverter
            .convertBytesToMessages(record.value())
            .getMessages()
            .stream()
            .map(EventuateKafkaMultiMessage::getValue)
            .map(KafkaMessage::new)
            .map(kafkaMessage ->
                    swimlaneBasedDispatcher.dispatch(new RawKafkaMessage(record.value()), record.partition(), message -> handle(message, callback, handler)))
            .collect(Collectors.toList()) // it is not possible to use "findAny()" now, because streams are lazy and only one message will be processed
            .stream()
            .findAny()
            .get();
  }

  private void handle(RawKafkaMessage message, BiConsumer<Void, Throwable> callback, ReactiveKafkaMessageHandler kafkaMessageHandler) {
    try {
        kafkaMessageHandler
                .apply(new KafkaMessage(EventuateBinaryMessageEncoding.bytesToString(message.getPayload())))
                .whenComplete((unused, throwable) -> callback.accept(null, throwable));
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

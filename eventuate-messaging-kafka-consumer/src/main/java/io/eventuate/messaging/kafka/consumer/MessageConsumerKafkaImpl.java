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
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class MessageConsumerKafkaImpl implements CommonMessageConsumer {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final String id = UUID.randomUUID().toString();

  private final String bootstrapServers;
  private final List<EventuateKafkaConsumer> consumers = new ArrayList<>();
  private final EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;
  private final KafkaConsumerFactory kafkaConsumerFactory;
  private final EventuateKafkaMultiMessageConverter eventuateKafkaMultiMessageConverter = new EventuateKafkaMultiMessageConverter();
  private final TopicPartitionToSwimlaneMapping partitionToSwimLaneMapping;

  public MessageConsumerKafkaImpl(String bootstrapServers,
                                  EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                  KafkaConsumerFactory kafkaConsumerFactory, TopicPartitionToSwimlaneMapping partitionToSwimLaneMapping) {
    this.bootstrapServers = bootstrapServers;
    this.eventuateKafkaConsumerConfigurationProperties = eventuateKafkaConsumerConfigurationProperties;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
    this.partitionToSwimLaneMapping = partitionToSwimLaneMapping;
  }

  public KafkaSubscription subscribe(String subscriberId, Set<String> channels, KafkaMessageHandler handler) {
    return subscribeWithReactiveHandler(subscriberId, channels, kafkaMessage -> {
      handler.accept(kafkaMessage);
      return CompletableFuture.completedFuture(null);
    });
  }

  public KafkaSubscription subscribeWithReactiveHandler(String subscriberId, Set<String> channels, ReactiveKafkaMessageHandler handler) {

    SwimlaneBasedDispatcher swimlaneBasedDispatcher = new SwimlaneBasedDispatcher(subscriberId, Executors.newCachedThreadPool(), partitionToSwimLaneMapping);

    EventuateKafkaConsumerMessageHandler kcHandler = (record, callback) -> {
      if (eventuateKafkaMultiMessageConverter.isMultiMessage(record.value())) {
        return handleBatch(record, swimlaneBasedDispatcher, callback, handler);
      } else {
        return swimlaneBasedDispatcher.dispatch(new RawKafkaMessage(record.key(), record.value()), new TopicPartition(record.topic(), record.partition()), message -> handle(message, callback, handler));
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
                    swimlaneBasedDispatcher.dispatch(new RawKafkaMessage(record.key(), record.value()), new TopicPartition(record.topic(), record.partition()), message -> handle(message, callback, handler)))
            .reduce((first, second) -> second)
            .get();
  }

  private void handle(RawKafkaMessage message, BiConsumer<Void, Throwable> callback, ReactiveKafkaMessageHandler kafkaMessageHandler) {
    try {
        kafkaMessageHandler
                .apply(new KafkaMessage(EventuateBinaryMessageEncoding.bytesToString(message.getPayload())))
                .get();
    } catch (RuntimeException e) {
      callback.accept(null, e);
      throw e;
    } catch (Throwable e) {
      callback.accept(null, e);
      throw new RuntimeException(e);
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

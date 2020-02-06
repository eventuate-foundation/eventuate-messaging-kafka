package io.eventuate.messaging.kafka.producer;

import io.eventuate.messaging.kafka.common.EventuateBinaryMessageEncoding;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class EventuateKafkaProducer {

  private Producer<String, byte[]> producer;
  private Properties producerProps;
  private StringSerializer stringSerializer = new StringSerializer();
  private EventuateKafkaPartitioner eventuateKafkaPartitioner = new EventuateKafkaPartitioner();

  public EventuateKafkaProducer(String bootstrapServers,
                                EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties) {

    producerProps = new Properties();
    producerProps.put("bootstrap.servers", bootstrapServers);
    producerProps.put("acks", "all");
    producerProps.put("retries", 0);
    producerProps.put("batch.size", 16384);
    producerProps.put("linger.ms", 1);
    producerProps.put("buffer.memory", 33554432);
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.putAll(eventuateKafkaProducerConfigurationProperties.getProperties());
    producer = new KafkaProducer<>(producerProps);
  }

  public CompletableFuture<?> send(String topic, String key, String body) {
    return send(topic, key, EventuateBinaryMessageEncoding.stringToBytes(body));
  }

  public CompletableFuture<?> send(String topic, int partition, String key, String body) {
    return send(topic, partition, key, EventuateBinaryMessageEncoding.stringToBytes(body));
  }

  public CompletableFuture<?> send(String topic, String key, byte[] bytes) {
    return send(new ProducerRecord<>(topic, key, bytes));
  }

  public CompletableFuture<?> send(String topic, int partition, String key, byte[] bytes) {
    return send(new ProducerRecord<>(topic, partition, key, bytes));
  }

  private CompletableFuture<?> send(ProducerRecord<String, byte[]> producerRecord) {
    CompletableFuture<Object> result = new CompletableFuture<>();
    producer.send(producerRecord, (metadata, exception) -> {
      if (exception == null)
        result.complete(metadata);
      else
        result.completeExceptionally(exception);
    });

    return result;
  }

  public int partitionFor(String topic, String key) {
    return eventuateKafkaPartitioner.partition(topic, stringSerializer.serialize(topic, key), partitionsFor(topic));
  }

  public List<PartitionInfo> partitionsFor(String topic) {
    return producer.partitionsFor(topic);
  }

  public void close() {
    producer.close(1, TimeUnit.SECONDS);
  }
}

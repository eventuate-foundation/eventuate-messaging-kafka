package io.eventuate.messaging.kafka.basic.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class DefaultKafkaMessageConsumer implements KafkaMessageConsumer {

  private final KafkaConsumer<String, byte[]> delegate;

  public static KafkaMessageConsumer create(Properties properties) {
    return new DefaultKafkaMessageConsumer(new KafkaConsumer<>(properties));
  }

  private DefaultKafkaMessageConsumer(KafkaConsumer<String, byte[]> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void assign(Collection<TopicPartition> topicPartitions) {
    delegate.assign(topicPartitions);
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> topicPartitions) {
    delegate.seekToEnd(topicPartitions);
  }

  @Override
  public long position(TopicPartition topicPartition) {
    return delegate.position(topicPartition);
  }

  @Override
  public void seek(TopicPartition topicPartition, long position) {
    delegate.seek(topicPartition, position);
  }

  @Override
  public void subscribe(List<String> topics) {
    delegate.subscribe(new ArrayList<>(topics));
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    delegate.subscribe(topics, callback);
  }

  @Override
  public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
    delegate.commitSync(offsets);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return delegate.partitionsFor(topic);
  }

  @Override
  public ConsumerRecords<String, byte[]> poll(Duration duration) {
    return delegate.poll(duration);
  }

  @Override
  public void pause(Set<TopicPartition> partitions) {
    delegate.pause(partitions);
  }

  @Override
  public void resume(Set<TopicPartition> partitions) {
    delegate.resume(partitions);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void close(Duration duration) {
    delegate.close(duration);
  }

}

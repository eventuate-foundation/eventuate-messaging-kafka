package io.eventuate.messaging.kafka.basic.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public interface KafkaMessageConsumer {

  void assign(Collection<TopicPartition> topicPartitions);

  void seekToEnd(Collection<TopicPartition> topicPartitions);

  long position(TopicPartition topicPartition);

  void seek(TopicPartition topicPartition, long position);

  void subscribe(List<String> topics);

  void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets);

  List<PartitionInfo> partitionsFor(String topic);

  ConsumerRecords<String, byte[]> poll(Duration duration);

  void pause(Set<TopicPartition> partitions);

  void resume(Set<TopicPartition> partitions);

  void close();

  void close(Duration duration);
}

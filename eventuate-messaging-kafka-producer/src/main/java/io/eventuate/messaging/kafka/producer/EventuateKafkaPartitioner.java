package io.eventuate.messaging.kafka.producer;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class EventuateKafkaPartitioner {

  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

  public int partition(String topic, byte[] keyBytes, List<PartitionInfo> partitions) {
    int numPartitions = partitions.size();
    if (keyBytes == null) {
      int nextValue = nextValue(topic);
      List<PartitionInfo> availablePartitions = partitions.stream().filter(p -> p.leader() != null).collect(Collectors.toList());
      if (availablePartitions.size() > 0) {
        int part = Utils.toPositive(nextValue) % availablePartitions.size();
        return availablePartitions.get(part).partition();
      } else {
        return Utils.toPositive(nextValue) % numPartitions;
      }
    } else {
      return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }
  }

  private int nextValue(String topic) {
    AtomicInteger counter = topicCounterMap.get(topic);
    if (null == counter) {
      counter = new AtomicInteger(ThreadLocalRandom.current().nextInt());
      AtomicInteger currentCounter = topicCounterMap.putIfAbsent(topic, counter);
      if (currentCounter != null) {
        counter = currentCounter;
      }
    }
    return counter.getAndIncrement();
  }
}
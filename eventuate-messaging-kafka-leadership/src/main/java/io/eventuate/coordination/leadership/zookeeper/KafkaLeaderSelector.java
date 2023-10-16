package io.eventuate.coordination.leadership.zookeeper;

import io.eventuate.coordination.leadership.EventuateLeaderSelector;
import io.eventuate.coordination.leadership.LeaderSelectedCallback;
import io.eventuate.messaging.kafka.basic.consumer.ConsumerPropertiesFactory;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.basic.consumer.KafkaMessageConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class KafkaLeaderSelector implements EventuateLeaderSelector {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final KafkaConsumerFactory kafkaConsumerFactory;
  private final String lockId;
  private final String leaderId;
  private final LeaderSelectedCallback leaderSelectedCallback;
  private final Runnable leaderRemovedCallback;
  private final String bootstrapServer;
  private volatile KafkaMessageConsumer consumer;

  private final Executor executor = Executors.newCachedThreadPool();
  private Timer timer;


  public KafkaLeaderSelector(String lockId,
                             LeaderSelectedCallback leaderSelectedCallback,
                             Runnable leaderRemovedCallback,
                             String bootstrapServer, KafkaConsumerFactory kafkaConsumerFactory) {

    this(lockId, UUID.randomUUID().toString(), leaderSelectedCallback, leaderRemovedCallback, bootstrapServer, kafkaConsumerFactory);
  }

  public KafkaLeaderSelector(String lockId,
                             String leaderId,
                             LeaderSelectedCallback leaderSelectedCallback,
                             Runnable leaderRemovedCallback, String bootstrapServer, KafkaConsumerFactory kafkaConsumerFactory) {
    this.lockId = lockId;
    this.leaderId = leaderId;
    this.leaderSelectedCallback = leaderSelectedCallback;
    this.leaderRemovedCallback = leaderRemovedCallback;
    this.bootstrapServer = bootstrapServer;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
  }

  @Override
  public void start() {
    logger.info("Starting leader selector: {}", leaderId);

    Properties consumerProperties = ConsumerPropertiesFactory.makeDefaultConsumerProperties(bootstrapServer, lockId);
    // Need to do something similar
    //this.consumerProperties.putAll(eventuateKafkaConsumerConfigurationProperties.getProperties());

    consumer = kafkaConsumerFactory.makeConsumer(lockId, consumerProperties);

    ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

      private boolean selected = false;

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked: {}", partitions);
        if (partitions.stream().anyMatch(tp -> tp.partition() == 0)) {
          executor.execute(this::noteLeadershipRevoked);
        }
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partitions assigned: {}", partitions);
        if (partitions.stream().anyMatch(tp -> tp.partition() == 0)) {
          executor.execute(this::noteLeadershipSelected);
          selected = true;
        }

      }

      private void noteLeadershipSelected() {
        logger.info("Leadership selected {}", leaderId);
        leaderSelectedCallback.run(() -> {
          if (selected) {
            selected = false;
            leaderRemovedCallback.run();
            stop();
            start();
          }
        });
      }

      private void noteLeadershipRevoked() {
        logger.info("Leadership revoked {}", leaderId);
        if (selected) {
          selected = false;
          leaderRemovedCallback.run();
        }
      }
    };

    consumer.subscribe(Collections.singletonList("eventuate-leadership-coordination"), rebalanceListener);

    timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        synchronized (consumer) {
          consumer.poll(Duration.ofMillis(5000));
        }
      }
    }, 0L, 5000L);

    logger.info("Started leader selector {}", leaderId);
  }

  @Override
  public void stop() {
    logger.info("Closing leader selector, leaderId : {}", leaderId);
    timer.cancel();
    synchronized (consumer) {
      consumer.close();
    }
    logger.info("Closed leader selector, leaderId : {}", leaderId);
  }
}

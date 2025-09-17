package io.eventuate.coordination.leadership.zookeeper;

import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import io.eventuate.common.testcontainers.ReusableNetworkFactory;
import io.eventuate.coordination.leadership.LeaderSelectedCallback;
import io.eventuate.coordination.leadership.tests.AbstractLeadershipTest;
import io.eventuate.messaging.kafka.basic.consumer.DefaultKafkaConsumerFactory;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.spring.common.EventuateKafkaPropertiesConfiguration;
import io.eventuate.messaging.kafka.testcontainers.EventuateKafkaContainer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;

import java.util.Collections;
import java.util.UUID;

@SpringBootTest(classes = KafkaLeadershipTest.Config.class)
public class KafkaLeadershipTest extends AbstractLeadershipTest<KafkaLeaderSelector> {

  @Configuration
  @EnableAutoConfiguration
  @Import({EventuateKafkaPropertiesConfiguration.class})
  public static class Config {

    @Bean
    @ConditionalOnMissingBean
    public KafkaConsumerFactory kafkaConsumerFactory() {
      return new DefaultKafkaConsumerFactory();
    }

  }

  @Autowired
  private KafkaConsumerFactory kafkaConsumerFactory;

  @Autowired
  private EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties;

  private static String networkName = "testnetwork";
  public static Network network = ReusableNetworkFactory.createNetwork(networkName);

  public static EventuateZookeeperContainer zookeeper = new EventuateZookeeperContainer().withReuse(true)
          .withNetwork(network)
          .withNetworkAliases("zookeeper");

  public static EventuateKafkaContainer kafka =
          new EventuateKafkaContainer("eventuateio/eventuate-kafka:0.19.0.BUILD-SNAPSHOT", zookeeper.getConnectionString())
                  .withNetwork(network)
                  .withNetworkAliases("kafka")
                  .withReuse(true);

  @DynamicPropertySource
  static void registerContainerProperties(DynamicPropertyRegistry registry) {
    PropertyProvidingContainer.startAndProvideProperties(registry, zookeeper, kafka);
  }

  private String lockId;
  private int leaderIdx;

  @BeforeEach
  public void init() {
    leaderIdx = 0;
    lockId = "/zk/lock/test/%s".formatted(UUID.randomUUID().toString());
  }

  @Override
  protected KafkaLeaderSelector createLeaderSelector(LeaderSelectedCallback leaderSelectedCallback, Runnable leaderRemovedCallback) {
    return new KafkaLeaderSelector(lockId,
            "random-tbd-leader-id-" + leaderIdx++,
            leaderSelectedCallback,
            leaderRemovedCallback,
            eventuateKafkaConfigurationProperties.getBootstrapServers(),
            Collections.emptyMap(), kafkaConsumerFactory);
  }
}

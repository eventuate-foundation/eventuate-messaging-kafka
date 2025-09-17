package io.eventuate.messaging.kafka.testcontainers;

import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import io.eventuate.messaging.kafka.consumer.KafkaSubscription;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import io.eventuate.messaging.kafka.spring.consumer.MessageConsumerKafkaConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;

import java.util.Collections;
import java.util.UUID;

@SpringBootTest(classes = EventuateKafkaContainerTest.Config.class)
public class EventuateKafkaContainerTest {

    public static Network network = Network.newNetwork();

    public static EventuateZookeeperContainer zookeeper = new EventuateZookeeperContainer().withReuse(true)
            .withNetwork(network)
            .withNetworkAliases("zookeeper");

    public static EventuateKafkaContainer kafka =
            EventuateKafkaContainer.makeFromDockerfile(zookeeper.getConnectionString())
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withReuse(true);

    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        PropertyProvidingContainer.startAndProvideProperties(registry, zookeeper, kafka);
    }

    @Configuration
    @Import({MessageConsumerKafkaConfiguration.class, KafkaConsumerFactoryConfiguration.class})
    public static class Config {
    }

    @Autowired
    private MessageConsumerKafkaImpl messageConsumerKafka;

    @Test
    public void showStart() {
        System.out.println(kafka.getLogs());
        KafkaSubscription s = messageConsumerKafka.subscribe(UUID.randomUUID().toString(), Collections.singleton(UUID.randomUUID().toString()), m -> {
        });
    }


}
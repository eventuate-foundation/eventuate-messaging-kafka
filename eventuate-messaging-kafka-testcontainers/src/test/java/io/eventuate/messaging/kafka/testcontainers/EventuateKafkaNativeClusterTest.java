package io.eventuate.messaging.kafka.testcontainers;

import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import io.eventuate.messaging.kafka.consumer.KafkaSubscription;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.spring.consumer.KafkaConsumerFactoryConfiguration;
import io.eventuate.messaging.kafka.spring.consumer.MessageConsumerKafkaConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Collections;
import java.util.UUID;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
public class EventuateKafkaNativeClusterTest {

    private static final EventuateKafkaNativeCluster kafkaCluster =
            new EventuateKafkaNativeCluster("network-" + uuid());

    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        PropertyProvidingContainer.startAndProvideProperties(registry, kafkaCluster.kafka);
    }

    @Configuration
    @Import({MessageConsumerKafkaConfiguration.class, KafkaConsumerFactoryConfiguration.class})
    public static class Config {
    }

    @Autowired
    private MessageConsumerKafkaImpl messageConsumerKafka;

    @Test
    public void shouldSubscribeToKafka() {

        KafkaSubscription s = messageConsumerKafka.subscribe(uuid(),
                Collections.singleton(uuid()),
                m -> {});

    }

    private static String uuid() {
        return UUID.randomUUID().toString();
    }

}
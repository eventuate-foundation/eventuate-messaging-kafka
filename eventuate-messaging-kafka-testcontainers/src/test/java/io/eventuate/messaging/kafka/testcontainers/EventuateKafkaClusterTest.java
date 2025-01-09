package io.eventuate.messaging.kafka.testcontainers;

import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class EventuateKafkaClusterTest {

    private static EventuateKafkaCluster eventuateKafkaCluster;

    @BeforeClass
    public static void startCluster() {
        eventuateKafkaCluster = new EventuateKafkaCluster("network-" + System.currentTimeMillis()) {
            @Override
            protected EventuateKafkaContainer makeEventuateKafkaContainer() {
                return EventuateKafkaContainer.makeFromDockerfile(zookeeper.getConnectionString());
            }
        }.withReuse(false);

        Startables.deepStart(eventuateKafkaCluster.kafka).join();
    }

    @Test
    public void shouldStartKafka() {

    }

    @Test
    public void shouldRegisterProperties() {
        Map<String, Object> properties = new HashMap<>();

        eventuateKafkaCluster.registerProperties(properties::put);

        assertThat(properties).containsKeys("eventuatelocal.kafka.bootstrap.servers",
            "eventuatelocal.zookeeper.connection.string");
    }

}
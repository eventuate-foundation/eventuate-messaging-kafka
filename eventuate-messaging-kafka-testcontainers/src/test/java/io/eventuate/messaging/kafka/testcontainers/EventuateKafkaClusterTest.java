package io.eventuate.messaging.kafka.testcontainers;

import org.junit.Test;
import org.testcontainers.lifecycle.Startables;

public class EventuateKafkaClusterTest {

    @Test
    public void shouldStartKafka() {
        EventuateKafkaCluster eventuateKafkaCluster = new EventuateKafkaCluster("network-" + System.currentTimeMillis()) {
            @Override
            protected EventuateKafkaContainer makeEventuateKafkaContainer() {
                return EventuateKafkaContainer.makeFromDockerfile(zookeeper.getConnectionString());
            }
        };

        try (EventuateKafkaContainer kafka = eventuateKafkaCluster.kafka) {
            Startables.deepStart(kafka).join();
        }

    }

}
package io.eventuate.messaging.kafka.testcontainers;

import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

public class EventuateKafkaCluster {

    public final Network network;

    public final EventuateZookeeperContainer zookeeper;

    public final EventuateKafkaContainer kafka;

    public EventuateKafkaCluster() {
        network = ReusableNetworkFactory.createNetwork("foofoo");
        zookeeper = new EventuateZookeeperContainer().withReuse(true)
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .waitingFor(Wait.forHealthcheck());
        kafka = new EventuateKafkaContainer("zookeeper:2181")
                .waitingFor(Wait.forHealthcheck())
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withReuse(true);
    }
}

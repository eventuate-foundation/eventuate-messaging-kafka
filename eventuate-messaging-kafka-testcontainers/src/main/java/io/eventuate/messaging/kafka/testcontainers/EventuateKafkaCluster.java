package io.eventuate.messaging.kafka.testcontainers;

import io.eventuate.common.testcontainers.EventuateZookeeperContainer;
import io.eventuate.common.testcontainers.ReusableNetworkFactory;
import org.testcontainers.containers.Network;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class EventuateKafkaCluster {

    public final Network network;

    public final EventuateZookeeperContainer zookeeper;

    public final EventuateKafkaContainer kafka;

    public EventuateKafkaCluster() {
        this("foofoo");
    }

    public EventuateKafkaCluster(String networkName) {
        network = ReusableNetworkFactory.createNetwork(networkName);
        zookeeper = new EventuateZookeeperContainer().withReuse(true)
            .withNetwork(network)
            .withNetworkAliases("zookeeper");
        kafka = makeEventuateKafkaContainer()
            .dependsOn(zookeeper)
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withReuse(true);
    }

    protected EventuateKafkaContainer makeEventuateKafkaContainer() {
        return new EventuateKafkaContainer("zookeeper:2181");
    }


    public void registerProperties(BiConsumer<String, Supplier<Object>> registry) {
        zookeeper.registerProperties(registry);
        kafka.registerProperties(registry);
    }

    public EventuateKafkaCluster withReuse(boolean reuse) {
        zookeeper.withReuse(reuse);
        kafka.withReuse(reuse);
        return this;
    }
}

package io.eventuate.messaging.kafka.testcontainers;

import io.eventuate.common.testcontainers.ReusableNetworkFactory;
import org.testcontainers.containers.Network;

public class EventuateKafkaNativeCluster {

    public final Network network;
    public final EventuateKafkaNativeContainer kafka;

    public EventuateKafkaNativeCluster() {
        this("foofoo");
    }

    public EventuateKafkaNativeCluster(String networkName) {
        network = ReusableNetworkFactory.createNetwork(networkName);

        kafka = (EventuateKafkaNativeContainer)new EventuateKafkaNativeContainer()
                .withNetwork(network)
        ;
    }


}

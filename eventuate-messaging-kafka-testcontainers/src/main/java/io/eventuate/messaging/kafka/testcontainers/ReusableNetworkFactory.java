package io.eventuate.messaging.kafka.testcontainers;

import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReusableNetworkFactory {
  // Hack from https://github.com/testcontainers/testcontainers-java/issues/3081#issuecomment-831119692
  static Network createNetwork(String networkName) {
    Network network = Network.newNetwork();
    ReflectionTestUtils.setField(network, "name", networkName);
    List<com.github.dockerjava.api.model.Network> networks =
            DockerClientFactory.instance().client().listNetworksCmd().withNameFilter(networkName).exec();
    if (!networks.isEmpty()) {
      ReflectionTestUtils.setField(network, "id", networks.get(0).getId());
      ((AtomicBoolean)ReflectionTestUtils.getField(network, "initialized")).set(true);
    }
    return network;
  }
}

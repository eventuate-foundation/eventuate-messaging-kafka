package io.eventuate.messaging.kafka.spring.basic.consumer;

import io.eventuate.messaging.kafka.basic.consumer.AbstractEventuateKafkaBasicConsumerTest;
import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.consumer.SwimlanePerTopicPartition;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EventuateKafkaBasicConsumerSpringWithSwimlanePerTopicTest.Config.class,
        properties = "eventuate.local.kafka.consumer.backPressure.high=3")
public class EventuateKafkaBasicConsumerSpringWithSwimlanePerTopicTest extends AbstractEventuateKafkaBasicConsumerTest  {

    @Configuration
    @EnableAutoConfiguration
    @Import(EventuateKafkaBasicConsumerSpringTest.EventuateKafkaConsumerTestConfiguration.class)
    public static class Config {
        @Bean
        public SwimlanePerTopicPartition swimlanePerTopicPartition() {
            return new SwimlanePerTopicPartition();
        }
    }

    @Autowired
    private EventuateKafkaConfigurationProperties kafkaProperties;

    @Autowired
    private EventuateKafkaConsumerConfigurationProperties consumerProperties;

    @Autowired
    private EventuateKafkaProducer producer;

    @Autowired
    private MessageConsumerKafkaImpl consumer;

    @Autowired
    private KafkaConsumerFactory kafkaConsumerFactory;
    @Override
    protected EventuateKafkaConfigurationProperties getKafkaProperties() {
        return kafkaProperties;
    }

    @Override
    protected EventuateKafkaConsumerConfigurationProperties getConsumerProperties() {
        return consumerProperties;
    }

    @Override
    protected EventuateKafkaProducer getProducer() {
        return producer;
    }

    @Override
    protected MessageConsumerKafkaImpl getConsumer() {
        return consumer;
    }

    @Override
    protected KafkaConsumerFactory getKafkaConsumerFactory() {
        return kafkaConsumerFactory;
    }

    @Test
    @Override
    public void shouldConsumeMessages() {
        super.shouldConsumeMessages();
    }

}

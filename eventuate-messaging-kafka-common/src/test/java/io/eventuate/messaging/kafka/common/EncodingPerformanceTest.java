package io.eventuate.messaging.kafka.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.eventuate.messaging.kafka.common.sbe.MessageHeaderDecoder;
import io.eventuate.messaging.kafka.common.sbe.MessageHeaderEncoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageDecoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;
import org.msgpack.MessagePack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

public class EncodingPerformanceTest {

  private EventuateKafkaMultiMessageKeyValue[] keyValues;

  private int nRecords = 10000;
  private int nIterations = 10000;

  @Test
  public void sbePerformanceTest() throws Exception {
    testPerformance(() -> sbeDecode(sbeEncode()));
  }

  @Test
  public void messageBuilderPerformanceTest() throws Exception {
    testPerformance(this::testMessageBuilder);
  }

  @Test
  public void msgpackPerformanceTest() throws Exception {
    testPerformance(this::testMsgpack);
  }

  @Test
  public void kryoPerformanceTest() throws Exception {
    testPerformance(this::testKryo);
  }

  public void testPerformance(Callable serialization) throws Exception {

    List<Double> times = new ArrayList<>();

    int counter = 0;
    for (int i = 0; i < nIterations; i++) {
      keyValues = new EventuateKafkaMultiMessageKeyValue[nRecords];
      for (int j = 0; j < nRecords; j++) {
        keyValues[j] = new EventuateKafkaMultiMessageKeyValue("key" + (counter++), "value" + (counter++));
      }
      double t = System.nanoTime();
      serialization.call();
      times.add((System.nanoTime() - t)/1000000d);
    }

    double min = times.stream().min(Double::compareTo).get();
    double max = times.stream().max(Double::compareTo).get();
    double average = times.stream().reduce((a, b) -> a + b).get() / times.size();
    times.remove(min);
    times.remove(max);
    double averageWithoutMinAndMax = times.stream().reduce((a, b) -> a + b).get() / times.size();
    System.out.println(String.format("min = %s", min));
    System.out.println(String.format("max = %s", max));
    System.out.println(String.format("average = %s", average));
    System.out.println(String.format("average without min and max = %s", averageWithoutMinAndMax));
  }


  public EventuateKafkaMultiMessageKeyValue[] testMsgpack() throws IOException {
    MessagePack messagePack = new MessagePack();

    byte[] raw = messagePack.write(keyValues);
    return messagePack.read(raw, EventuateKafkaMultiMessageKeyValue[].class);
  }

  public List<EventuateKafkaMultiMessageKeyValue> testKryo() {
    Kryo kryo = new Kryo();
    Output output = new Output(1000000);
    Arrays.stream(keyValues).forEach(kv -> {
      kryo.writeObject(output, kv.getKey());
      kryo.writeObject(output, kv.getValue());
    });
    byte[] raw = output.getBuffer();
    List<EventuateKafkaMultiMessageKeyValue> result = new ArrayList<>();
    Input input = new Input(raw);
    result.add(new EventuateKafkaMultiMessageKeyValue(kryo.readObject(input, String.class), kryo.readObject(input, String.class)));
    return result;
  }


  public List<EventuateKafkaMultiMessageKeyValue> testMessageBuilder() {
    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(1000000);
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    Arrays.stream(keyValues).forEach(messageBuilder::addMessage);

    byte[] serializedMessages = messageBuilder.toBinaryArray();

    return eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);
  }

  private ExpandableArrayBuffer sbeEncode() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1000);

    MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    MultiMessageEncoder multiMessageEncoder = new MultiMessageEncoder().wrapAndApplyHeader(buffer, 0, messageHeaderEncoder);

    MultiMessageEncoder.MessagesEncoder messagesEncoder = multiMessageEncoder.messagesCount(keyValues.length);

    Arrays.stream(keyValues).forEach(keyValue -> messagesEncoder.next().key(keyValue.getKey()).value(keyValue.getValue()));

    int messageLength = MessageHeaderEncoder.ENCODED_LENGTH + multiMessageEncoder.encodedLength();

    ExpandableArrayBuffer buffer1 = new ExpandableArrayBuffer(1000);
    buffer1.putBytes(0, buffer, 0, messageLength);
    return buffer1;
  }

  private List<EventuateKafkaMultiMessageKeyValue> sbeDecode(ExpandableArrayBuffer buffer) {
    MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

    messageHeaderDecoder.wrap(buffer, 0);

    final int templateId = messageHeaderDecoder.templateId();
    final int actingBlockLength = messageHeaderDecoder.blockLength();
    final int actingVersion = messageHeaderDecoder.version();


    MultiMessageDecoder multiMessageDecoder = new MultiMessageDecoder().wrap(buffer, messageHeaderDecoder.encodedLength(), actingBlockLength, actingVersion);

    MultiMessageDecoder.MessagesDecoder messages = multiMessageDecoder.messages();
    int count = messages.count();

    assertEquals(keyValues.length, count);


    List<EventuateKafkaMultiMessageKeyValue> multiMessageKeyValues = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      messages.next();
      int keyLength = messages.keyLength();
      byte[] keyBytes = new byte[keyLength];
      messages.getKey(keyBytes, 0, keyLength);
      int valueLength = messages.valueLength();
      byte[] valueBytes = new byte[valueLength];
      messages.getKey(valueBytes, 0, valueLength);

      String key = new String(keyBytes);
      String value = new String(valueBytes);

      multiMessageKeyValues.add(new EventuateKafkaMultiMessageKeyValue(key, value));
    }

    return multiMessageKeyValues;
  }
}

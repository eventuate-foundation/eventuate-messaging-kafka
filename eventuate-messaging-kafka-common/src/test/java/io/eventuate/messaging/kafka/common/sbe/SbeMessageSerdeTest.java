package io.eventuate.messaging.kafka.common.sbe;

import org.agrona.ExpandableArrayBuffer;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class SbeMessageSerdeTest {

  String[] keys = new String[]{"abcd", "key2"};
  String[] values = new String[]{"Tim", "Value"};

  @Test
  public void shouldDoSomething() {

    ExpandableArrayBuffer buffer = encode();

    decode(buffer);
  }

  private ExpandableArrayBuffer encode() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1000);

    MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    MultiMessageEncoder multiMessageEncoder = new MultiMessageEncoder().wrapAndApplyHeader(buffer, 0, messageHeaderEncoder);

    MultiMessageEncoder.MessagesEncoder messagesEncoder = multiMessageEncoder.messagesCount(keys.length);

    IntStream.range(0, keys.length).forEach(idx -> {
              messagesEncoder
                      .next().key(keys[idx]).value(values[idx]);
            }
    );


    int messageLength = MessageHeaderEncoder.ENCODED_LENGTH + multiMessageEncoder.encodedLength();
    System.out.println("Encoded length=" + messageLength);

    // messageHeaderEncoder.wrap(buffer, 0).magicBytes(0, (short)10);

    ExpandableArrayBuffer buffer1 = new ExpandableArrayBuffer(1000);
    buffer1.putBytes(0, buffer, 0, messageLength);
    return buffer1;
  }

  private void decode(ExpandableArrayBuffer buffer) {
    MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

    messageHeaderDecoder.wrap(buffer, 0);

    final int templateId = messageHeaderDecoder.templateId();
    final int actingBlockLength = messageHeaderDecoder.blockLength();
    final int actingVersion = messageHeaderDecoder.version();


    MultiMessageDecoder multiMessageDecoder = new MultiMessageDecoder().wrap(buffer, messageHeaderDecoder.encodedLength(), actingBlockLength, actingVersion);

    MultiMessageDecoder.MessagesDecoder messages = multiMessageDecoder.messages();
    int count = messages.count();

    assertEquals(keys.length, count);

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

      assertEquals(keys[i], key);
      assertEquals(values[i], value);


    }
  }
}

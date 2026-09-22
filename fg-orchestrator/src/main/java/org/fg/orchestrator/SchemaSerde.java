package org.fg.orchestrator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/** Schema ↔ bytes（Arrow IPC schema message 序列化）。 */
public final class SchemaSerde {

  private SchemaSerde() {}

  public static byte[] serialize(Schema schema) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (WriteChannel channel = new WriteChannel(Channels.newChannel(out))) {
      MessageSerializer.serialize(channel, schema);
    } catch (IOException e) {
      throw new IllegalStateException("Schema serialization failed", e);
    }
    return out.toByteArray();
  }

  public static Schema deserialize(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      Schema schema =
          MessageSerializer.deserializeSchema(
              new org.apache.arrow.vector.ipc.ReadChannel(
                  java.nio.channels.Channels.newChannel(
                      new java.io.ByteArrayInputStream(bytes))));
      if (schema == null) {
        throw new IllegalStateException("Schema deserialization failed");
      }
      return schema;
    } catch (IOException e) {
      throw new IllegalStateException("Schema deserialization failed", e);
    }
  }
}

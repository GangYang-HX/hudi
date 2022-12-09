/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.commit.serializer;

import org.apache.hudi.commit.policy.PartitionState;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PartitionStateSerializer implements SimpleVersionedSerializer<PartitionState> {

  private static final int VERSION = 1;
  public static final PartitionStateSerializer INSTANCE = new PartitionStateSerializer();

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(PartitionState partitionState) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputViewStreamWrapper(baos);
    out.writeInt(VERSION);
    byte[] bodyBytes = serializeBody(partitionState);
    out.writeInt(bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
    byte[] serializeResult = baos.toByteArray();
    return serializeResult;
  }

  public byte[] serializeBody(PartitionState partitionState) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(partitionState);
      oos.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public PartitionState deserialize(int version, byte[] serialized) throws IOException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
         DataInputStream in = new DataInputViewStreamWrapper(bais)) {
      int currentVersion = in.readInt();
      int bodySize = in.readInt();
      byte[] bodyBytes = new byte[bodySize];
      in.readFully(bodyBytes);
      if (currentVersion == version) {
        return deserializeV1(bodyBytes);
      }

    }
    throw new IOException("Unknown version: " + version);
  }

  private PartitionState deserializeV1(byte[] serialized) throws IOException {
    try (ObjectInputStream in =
             new ObjectInputStream(new ByteArrayInputStream(serialized))) {
      return (PartitionState) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to deserialize PartitionStateSerializer", e);
    }
  }
}

/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.flink.tiering.source.TableBucketWriteResult;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.util.function.SerializableSupplier;

import java.io.IOException;

/** A {@link TypeInformation} for {@link CommittableMessage}. */
public class CommittableMessageTypeInfo<Committable>
        extends TypeInformation<CommittableMessage<Committable>> {

    private final SerializableSupplier<SimpleVersionedSerializer<Committable>>
            committableSerializerFactory;

    private CommittableMessageTypeInfo(
            SerializableSupplier<SimpleVersionedSerializer<Committable>>
                    committableSerializerFactory) {
        this.committableSerializerFactory = committableSerializerFactory;
    }

    public static <Committable> TypeInformation<CommittableMessage<Committable>> of(
            SerializableSupplier<SimpleVersionedSerializer<Committable>>
                    committableSerializerFactory) {
        return new CommittableMessageTypeInfo<>(committableSerializerFactory);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Class<CommittableMessage<Committable>> getTypeClass() {
        return (Class) TableBucketWriteResult.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<CommittableMessage<Committable>> createSerializer(
            ExecutionConfig executionConfig) {
        // no copy, so that data from writer is directly going into upstream operator while chaining
        SimpleVersionedSerializer<Committable> committableSerializer =
                committableSerializerFactory.get();
        return new SimpleVersionedSerializerTypeSerializerProxy<CommittableMessage<Committable>>(
                () ->
                        new org.apache.flink.core.io.SimpleVersionedSerializer<
                                CommittableMessage<Committable>>() {
                            @Override
                            public int getVersion() {
                                return committableSerializer.getVersion();
                            }

                            @Override
                            public byte[] serialize(
                                    CommittableMessage<Committable> committableCommittableMessage)
                                    throws IOException {
                                return committableSerializer.serialize(
                                        committableCommittableMessage.committable());
                            }

                            @Override
                            public CommittableMessage<Committable> deserialize(
                                    int version, byte[] serialized) throws IOException {
                                return new CommittableMessage<>(
                                        committableSerializer.deserialize(version, serialized));
                            }
                        }) {
            // nothing
            @Override
            public CommittableMessage<Committable> copy(CommittableMessage<Committable> from) {
                return from;
            }

            @Override
            public CommittableMessage<Committable> copy(
                    CommittableMessage<Committable> from, CommittableMessage<Committable> reuse) {
                return from;
            }
        };
    }

    @Override
    public String toString() {
        return "LakeCommittableTypeInfo";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CommittableMessageTypeInfo;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CommittableMessageTypeInfo;
    }
}

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

import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.util.function.SerializableSupplier;

import java.io.IOException;

/** A {@link TypeInformation} for {@link CommittableTypeInfo}. */
public class CommittableTypeInfo<Committable> extends TypeInformation<Committable> {

    private final SerializableSupplier<SimpleVersionedSerializer<Committable>>
            committableSerializerFactory;

    private CommittableTypeInfo(
            SerializableSupplier<SimpleVersionedSerializer<Committable>>
                    committableSerializerFactory) {
        this.committableSerializerFactory = committableSerializerFactory;
    }

    public static <Committable> TypeInformation<Committable> of(
            SerializableSupplier<SimpleVersionedSerializer<Committable>>
                    committableSerializerFactory) {
        return new CommittableTypeInfo<>(committableSerializerFactory);
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

    @Override
    public Class<Committable> getTypeClass() {
        return null;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Committable> createSerializer(ExecutionConfig executionConfig) {
        // no copy, so that data from writer is directly going into committer while chaining
        return new SimpleVersionedSerializerTypeSerializerProxy<Committable>(
                () ->
                        new org.apache.flink.core.io.SimpleVersionedSerializer<Committable>() {
                            private final SimpleVersionedSerializer<Committable>
                                    committableSerializer = committableSerializerFactory.get();

                            @Override
                            public int getVersion() {
                                return committableSerializer.getVersion();
                            }

                            @Override
                            public byte[] serialize(Committable committable) throws IOException {
                                return committableSerializer.serialize(committable);
                            }

                            @Override
                            public Committable deserialize(int version, byte[] bytes)
                                    throws IOException {
                                return committableSerializer.deserialize(version, bytes);
                            }
                        }) {
            // nothing
            @Override
            public Committable copy(Committable from) {
                return from;
            }

            @Override
            public Committable copy(Committable from, Committable reuse) {
                return from;
            }
        };
    }

    @Override
    public String toString() {
        return "CommittableTypeInfo";
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CommittableTypeInfo;
    }
}

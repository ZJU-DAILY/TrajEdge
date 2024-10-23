/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.example.trajstore.rocksdb;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;
import org.apache.storm.shade.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RocksDbKey implements Comparable<RocksDbKey> {
    static final int KEY_SIZE = 12 + 1;
    private static final Logger LOG = LoggerFactory.getLogger(RocksDbKey.class);
    private static Map<Byte, RocksDbKey> PREFIX_MAP = new HashMap<>();

    static {
        // pregenerate commonly used keys for scans
        for (KeyType type : EnumSet.allOf(KeyType.class)) {
            RocksDbKey key = new RocksDbKey(type, 0);
            PREFIX_MAP.put(type.getValue(), key);
        }
        PREFIX_MAP = Collections.unmodifiableMap(PREFIX_MAP);
    }

    private byte[] key;

    RocksDbKey(KeyType type, int metadataStringId) {
        byte[] key = new byte[KEY_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(key);
        bb.put(type.getValue());
        // part of trajectory id
        bb.putInt(metadataStringId);
        this.key = key;
    }

    /**
     * Constructor for a RocksDB key from raw data.
     *
     * @param raw the key data
     */
    RocksDbKey(byte[] raw) {
        this.key = raw;
    }

    /**
     * get the type of key.
     *
     * @return the type of key
     */
    KeyType getType() {
        return KeyType.getKeyType(key[0]);
    }

    /**
     * get the metadata string Id portion of the key for metadata keys.
     *
     * @return the metadata string Id
     * @throws RuntimeException if the key is not a metadata type
     */
    int getMetadataStringId() {
        if (this.getType().getValue() < KeyType.METADATA_STRING_END.getValue()) {
            return ByteBuffer.wrap(key, 1, 4).getInt();
        } else {
            throw new RuntimeException("Cannot fetch metadata string for key of type " + this.getType());
        }
    }

    /**
     * Get a zeroed key of the specified type.
     *
     * @param type the desired type
     * @return a key of the desired type
     */
    static RocksDbKey getPrefix(KeyType type) {
        return PREFIX_MAP.get(type.getValue());
    }

    /**
     * gets the first possible key value for the desired key type.
     *
     * @return the initial key
     */
    static RocksDbKey getInitialKey(KeyType type) {
        return PREFIX_MAP.get(type.getValue());
    }

    /**
     * gets the key just larger than the last possible key value for the desired key type.
     *
     * @return the last key
     */
    static RocksDbKey getLastKey(KeyType type) {
        byte value = (byte) (type.getValue() + 1);
        return PREFIX_MAP.get(value);
    }

    /**
     * Creates a metric key with the desired properties.
     *
     * @return the generated key
     */
    static RocksDbKey createTrajPointKey(int trajId, long timestamp) {
        byte[] raw = new byte[KEY_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(raw);
        bb.put(KeyType.METRIC_DATA.getValue());
        bb.putInt(trajId);
        bb.putLong(timestamp);

        return new RocksDbKey(raw);
    }

    /**
     * get the raw key bytes
     */
    byte[] getRaw() {
        return this.key;
    }


    /**
     * compares to keys on a byte by byte basis.
     *
     * @return comparison of key byte values
     */
    @Override
    public int compareTo(RocksDbKey o) {
        return UnsignedBytes.lexicographicalComparator().compare(this.getRaw(), o.getRaw());
    }

    long getTimestamp() {
        return ByteBuffer.wrap(key, 5, 8).getLong();
    }

    int getTrajId() {
        return ByteBuffer.wrap(key, 1, 4).getInt();
    }


    @Override
    public String toString() {
        return "[0x" + DatatypeConverter.printHexBinary(key) + "]";
    }
}


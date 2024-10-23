package org.example.datatypes;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * This class is a wrapper around a byte array to ensure equals and hashcode
 * operations use the values of the bytes rather than explicit object identity
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public class ByteArray implements Serializable, Comparable<ByteArray> {

  private byte[] bytes;

  public ByteArray(byte[] bytes) {
    this.bytes = bytes;
  }

  public ByteArray(List<byte[]> bytesList) {
    int arrLen = 0;
    for (byte[] value : bytesList) {
      arrLen += value.length;
    }
    bytes = new byte[arrLen];
    int offset = 0;
    for (byte[] value : bytesList) {
      System.arraycopy(value, 0, bytes, offset, value.length);
      offset += value.length;
    }
  }

  public ByteArray(ByteBuffer byteBuffer) {
    this.bytes = byteBuffer.array();
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    StringBuilder binaryString = new StringBuilder();
    for (byte b : bytes) {
      String binary = String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
      binaryString.append(binary);
    }
    return binaryString.toString();
    // return Bytes.toHex(bytes);
  }

  @Override
  public int compareTo(ByteArray o) {
    if (o == null) {
      return -1;
    }
    // lexicographical order
    for (int i = 0, j = 0; (i < bytes.length) && (j < o.bytes.length); i++, j++) {
      final int a = (bytes[i] & 0xff);
      final int b = (o.bytes[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return bytes.length - o.bytes.length;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + Arrays.hashCode(bytes);
    return result;
  }

  @Override
  public boolean equals(final Object obj ) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ByteArray other = (ByteArray) obj;
    return Arrays.equals(bytes, other.bytes);
  }

  /**
   * @return The offset of the byte buffer returned is set to the last position,
   * if you want to read values from the ByteBuffer, remember to flip the returned object.
   */
  public ByteBuffer toByteBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    return buffer;
  }
}

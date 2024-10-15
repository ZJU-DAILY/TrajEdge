package org.example.coding;

import org.example.coding.poscode.PosCode;
import org.example.coding.poscode.PosCodeRange;
import org.example.coding.sfc.SFCRange;
import org.example.coding.sfc.TimeIndexRange;
import org.example.datatypes.ByteArray;

import java.nio.ByteBuffer;

/**
 * 编码范围，边界以byte array存储，更通用。
 *
 * @author Haocheng Wang Created on 2022/11/13
 */
public class CodingRange {

  ByteArray lower;
  ByteArray upper;
  boolean validated;

  public CodingRange(ByteArray lower, ByteArray upper, boolean validated) {
    this.lower = lower;
    this.upper = upper;
    this.validated = validated;
  }

  public CodingRange() {
  }

  public ByteArray getLower() {
    return lower;
  }

  public ByteArray getUpper() {
    return upper;
  }

  public boolean isValidated() {
    return validated;
  }

  public void concatSfcRange(SFCRange sfcRange) {
    if (lower == null || upper == null) {
      lower = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.lower));
      upper = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.upper));
      validated = sfcRange.validated;
    } else {
      lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + Long.SIZE / Byte.SIZE)
          .put(lower.getBytes())
          .putLong(sfcRange.lower));
      upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + Long.SIZE / Byte.SIZE)
          .put(upper.getBytes())
          .putLong(sfcRange.upper));
      validated = sfcRange.validated;
    }
  }

  public void concatTimeIndexRange(TimeIndexRange timeIndexRange) {
    if (lower == null || upper == null) {
      lower = new ByteArray(
          ByteBuffer.allocate(XZTCoding.BYTES_NUM).putLong(timeIndexRange.getLowerXZTCode()));
      upper = new ByteArray(
          ByteBuffer.allocate(XZTCoding.BYTES_NUM).putLong(timeIndexRange.getUpperXZTCode()));
    } else {
      lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + XZTCoding.BYTES_NUM)
          .put(lower.getBytes())
          .putLong(timeIndexRange.getLowerXZTCode()));
      upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + XZTCoding.BYTES_NUM)
          .put(upper.getBytes())
          .putLong(timeIndexRange.getUpperXZTCode()));
    }
    validated = timeIndexRange.isContained();
  }

 public void concatPosCodeRange(PosCodeRange posCodeRange) {
   if (lower == null || upper == null) {
     lower = new ByteArray(ByteBuffer.allocate(PosCode.SIZE / Byte.SIZE).put(posCodeRange.lower.getPoscodeByte()));
     upper = new ByteArray(ByteBuffer.allocate(PosCode.SIZE / Byte.SIZE).put(posCodeRange.upper.getPoscodeByte()));
   } else {
     lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + PosCode.SIZE / Byte.SIZE)
         .put(lower.getBytes())
         .put(posCodeRange.lower.getPoscodeByte()));
     upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + PosCode.SIZE / Byte.SIZE)
         .put(upper.getBytes())
         .put(posCodeRange.upper.getPoscodeByte()));
   }
 }

  @Override
  public String toString() {
    return "CodingRange{" +
        "lower=" + lower +
        ", upper=" + upper +
        ", contained=" + validated +
        '}';
  }
}

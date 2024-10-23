package org.example.coding.poscode;

/**
 * @author Haocheng Wang
 * Created on 2022/11/13
 */
public class PosCodeRange implements Comparable<PosCodeRange> {
  public PosCode lower;
  public PosCode upper;

  public PosCodeRange() {
  }

  public PosCodeRange(PosCode lower, PosCode upper) {
    this.lower = lower;
    this.upper = upper;
  }

  public PosCode getLower() {
    return lower;
  }

  public void setLower(PosCode lower) {
    this.lower = lower;
  }

  public PosCode getUpper() {
    return upper;
  }

  public void setUpper(PosCode upper) {
    this.upper = upper;
  }

  @Override
  public int compareTo(PosCodeRange o) {
    int c1 = Byte.compare(lower.getPoscodeByte(), o.lower.getPoscodeByte());
    if (c1 != 0) {
      return c1;
    }
    return Byte.compare(upper.getPoscodeByte(), o.upper.getPoscodeByte());
  }

  @Override
  public String toString() {
    return "PosCodeRange{" +
        "lower=" + lower +
        ", upper=" + upper +
        '}';
  }
}

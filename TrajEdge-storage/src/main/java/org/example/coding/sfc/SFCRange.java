package org.example.coding.sfc;

import java.util.Objects;

/**
 * 根据查询条件生成的SFC范围，其中lower、upper组成了左闭、右闭的区间。contained为True时，代表在此区间内的所有对象均一定满足
 * 满足查询条件；为False时，代表在此区间内的所有对象可能不满足查询条件。
 * @author Haocheng Wang
 * Created on 2022/11/2
 */
public class SFCRange implements Comparable<SFCRange>{
  public long lower;
  public long upper;
  public boolean validated;

  public SFCRange(long lower, long upper, boolean validated) {
    this.lower = lower;
    this.upper = upper;
    this.validated = validated;
  }

  public static SFCRange apply(long lower, long upper, boolean contained) {
    if (contained) {
      return new CoveredSFCRange(lower, upper);
    } else {
      return new OverlapSFCRange(lower, upper);
    }
  }


  @Override
  public int compareTo(SFCRange o) {
    int c1 = Long.compare(lower, o.lower);
    if (c1 != 0) {
      return c1;
    }
    return Long.compare(upper, o.upper);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SFCRange sfcRange = (SFCRange) o;
    return lower == sfcRange.lower && upper == sfcRange.upper && validated == sfcRange.validated;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lower, upper, validated);
  }

  @Override
  public String toString() {
    return "SFCRange{" +
        "lower=" + lower +
        ", upper=" + upper +
        ", contained=" + validated +
        '}';
  }
}


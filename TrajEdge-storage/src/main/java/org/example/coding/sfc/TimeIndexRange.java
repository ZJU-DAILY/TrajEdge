package org.example.coding.sfc;

import java.util.Objects;

/**
 * @author Xu Qi
 * @since 2022/10/13
 */
public class TimeIndexRange {

  private  long lowerXZTCode;
  private  long upperXZTCode;
  private boolean contained;

  public TimeIndexRange(long lowerXZTCode, long upperXZTCode, boolean contained) {
   this.lowerXZTCode = lowerXZTCode;
   this.upperXZTCode = upperXZTCode;
   this.contained = contained;
  }


  public long getLowerXZTCode() {
    return lowerXZTCode;
  }

  public long getUpperXZTCode() {
    return upperXZTCode;
  }

  public boolean isContained() {
    return contained;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimeIndexRange that = (TimeIndexRange) o;
    return lowerXZTCode == that.lowerXZTCode && upperXZTCode == that.upperXZTCode && contained == that.contained;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lowerXZTCode, upperXZTCode, contained);
  }

  @Override
  public String toString() {
    return "TimeIndexRange{" + "lower=" + lowerXZTCode + ", upper=" + upperXZTCode + ", contained=" + contained + '}';
  }
}

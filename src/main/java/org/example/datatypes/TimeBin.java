package org.example.datatypes;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

import static org.example.constant.DBConstants.TIME_ZONE;

/**
 * @author Haocheng Wang Created on 2022/9/28
 */
public class TimeBin {

  /**
   * 将连续时间分割为多个紧密相连的bucket, 单位为 day, week, month, year
   */
  private final int binID;
  private final TimePeriod timePeriod;
  @SuppressWarnings("checkstyle:StaticVariableName")
  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, TIME_ZONE);

  public TimeBin(int binID, TimePeriod timePeriod) {
    this.binID = binID;
    this.timePeriod = timePeriod;
  }

  public int getBinID() {
    return binID;
  }

  public TimePeriod getTimePeriod() {
    return timePeriod;
  }

  /**
   * @return min date time of the bin (inclusive)
   */
  public ZonedDateTime getBinStartTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, binID);
  }

  /**
   * @return max date time of the bin(exculsive)
   */
  public ZonedDateTime getBinEndTime() {
    return timePeriod.getChronoUnit().addTo(Epoch, binID + 1);
  }

  /**
   * 获取refTime相对当前Bin起始时间的相对值（秒）
   * @param time 任意时间
   * @return time相对当前Bin起始时间的秒数
   */
  public long getRefTime(ZonedDateTime time) {
    return time.toEpochSecond() - getBinStartTime().toEpochSecond();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeBin timeBin = (TimeBin) o;
    return binID == timeBin.binID && timePeriod == timeBin.timePeriod;
  }

  @Override
  public int hashCode() {
    return Objects.hash(binID, timePeriod);
  }

  @Override
  public String toString() {
    return "TimeBin{" + "start=" + getBinStartTime() + ", timePeriod=" + timePeriod + '}';
  }
}

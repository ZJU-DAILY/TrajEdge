package org.example.datatypes;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * @author Xu Qi
 * @since 2022/10/6
 */
public class TimeLine {

  private ZonedDateTime timeStart;
  private ZonedDateTime timeEnd;
  private double reTimeStart;
  private double reTimeEnd;

  public TimeLine(ZonedDateTime timeStart, ZonedDateTime timeEnd) {
    if (timeStart.compareTo(timeEnd) < 0) {
      this.timeStart = timeStart;
      this.timeEnd = timeEnd;
    } else {
      this.timeStart = timeEnd;
      this.timeEnd = timeStart;
    }
  }

  public TimeLine(double reTimeStart, double reTimeEnd) {
    this.reTimeStart = reTimeStart;
    this.reTimeEnd = reTimeEnd;
  }

  public double getReTimeStart() {
    return reTimeStart;
  }

  public double getReTimeEnd() {
    return reTimeEnd;
  }

  public ZonedDateTime getTimeStart() {
    return timeStart;
  }

  public void setTimeStart(ZonedDateTime timeStart) {
    this.timeStart = timeStart;
  }

  public ZonedDateTime getTimeEnd() {
    return timeEnd;
  }

  public void setTimeEnd(ZonedDateTime timeEnd) {
    this.timeEnd = timeEnd;
  }

  public boolean contain(TimeLine timeLine) {
    return lessOrEqualTo(timeStart, timeLine.timeStart) && lessOrEqualTo(timeLine.timeEnd, timeEnd);
  }

  public boolean intersect(TimeLine other) {
    return lessOrEqualTo(timeStart, other.timeEnd) && lessOrEqualTo(other.timeStart, timeEnd);
  }

  private boolean lessOrEqualTo(ZonedDateTime t1, ZonedDateTime t2) {
    LocalDateTime ldt1 = t1.toLocalDateTime();
    LocalDateTime ldt2 = t2.toLocalDateTime();
    return ldt1.isBefore(ldt2) || ldt1.isEqual(ldt2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeLine timeLine = (TimeLine) o;
    return timeStart.equals(timeLine.timeStart) && timeEnd.equals(timeLine.timeEnd);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeStart, timeEnd);
  }

  @Override
  public String toString() {
    return "TimeLine{" + "timeStart=" + timeStart.toLocalDateTime() + ", timeEnd=" + timeEnd.toLocalDateTime() + '}';
  }
  public String toReString() {
    return "TimeLine{" + "timeStart=" + reTimeStart + ", timeEnd=" + reTimeEnd + '}';
  }
}

package org.example.coding.sfc;

import org.example.datatypes.TimeBin;
import org.example.datatypes.TimeElement;
import org.example.datatypes.TimeLine;
import org.example.datatypes.TimePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;

import static org.example.constant.CodingConstants.LOG_FIVE;
import static org.example.constant.DBConstants.TIME_ZONE;

/**
 * @author Xu Qi
 * @since 2022/10/6
 */
public class XZTSFC implements Serializable {
  // Time zone
  private final int g;
  private final TimePeriod timePeriod;
  private static final Logger LOGGER = LoggerFactory.getLogger(XZTSFC.class);

  static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, TIME_ZONE);
  private final TimeElement initXZTElement = new TimeElement(0, 1);
  TimeElement levelSeparator = new TimeElement(-1, -1);

  public XZTSFC(int g, TimePeriod timePeriod) {
    this.g = g;
    this.timePeriod = timePeriod;
  }

  public static XZTSFC apply(int g, TimePeriod timePeriod) {
    return new XZTSFC(g, timePeriod);
  }

  /**
   *
   * @return 单个Bin内的XZT单元数量
   */
  public int binElementCnt() {
   return (int) Math.pow(2, g+1) - 1;
  }

  public TimeBin getTimeBin(long xztCode) {
    int binID = (int) (xztCode / binElementCnt());
    return new TimeBin(binID, timePeriod);
  }

  /**
   * Get time coding
   *
   * @param timeLine With time starting point and end point information
   * @param timeBin  Time interval information
   * @return Time coding
   */
  public long index(TimeLine timeLine, TimeBin timeBin) {
    double[] binTime = normalize(timeLine.getTimeStart(), timeLine.getTimeEnd(), timeBin, true);
    double length = binTime[1] - binTime[0];
    int l1 = (int) Math.floor(Math.log(length) / LOG_FIVE);
    int level;
    if (l1 >= g) {
      level = g;
    } else {
      double length2 = Math.pow(0.5, l1 + 1);
      if (predicate(binTime[0], binTime[1], length2)) {
        level = l1 + 1;
      } else {
        level = l1;
      }
    }
    long elementCode = sequenceCode(binTime[0], level);
    long binBase = (long) binElementCnt() * timeBin.getBinID();
    return elementCode + binBase;
  }

  public List<TimeIndexRange> ranges(TimeLine timeQuery, Boolean isContainQuery) {
    List<TimeIndexRange> ranges = new ArrayList<>(5000);
    Deque<TimeElement> remaining = new ArrayDeque<>(5000);
    List<TimeBin> timeBinList = getTimeBinList(timeQuery, isContainQuery);
    for (TimeBin timeBin : timeBinList) {
      TimeLine referencedQuery = queryRefBin(timeQuery, timeBin);
      short level = 0;
      // 经典的层次遍历，还加了分隔符
      remaining.add(initXZTElement);
      remaining.add(levelSeparator);
      while (level < g && !remaining.isEmpty()) {
        TimeElement next = remaining.poll();
        if (next.equals(levelSeparator)) {
          // we've fully processed a level, increment our state
          if (!remaining.isEmpty()) {
            level = (short) (level + 1);
            remaining.add(levelSeparator);
          }
        } else {
          if (isContainQuery) {
            ranges = containSequenceCodeRange(next, referencedQuery, level, ranges, remaining, timeBin);
          } else {
            ranges = overlapSequenceCodeRange(next, referencedQuery, level, ranges, remaining, timeBin);
          }
        }
      }
      ranges = getMaxLevelSequenceCodeRange(level, referencedQuery, ranges, remaining, timeBin);
    }
    // ranges = mergeCodeRange(ranges);
    return ranges;
  }

  public List<TimeIndexRange> ranges(List<TimeLine> timeQueries, Boolean isContainQuery) {
    Set<TimeIndexRange> result = new HashSet<>();
    for (TimeLine query : timeQueries) {
      result.addAll(ranges(query, isContainQuery));
    }
    return new ArrayList<>(result);
  }

  public Boolean isOverlapped(TimeElement timeElement, List<TimeLine> timeLineList) {
    int i = 0;
    while (i < timeLineList.size()) {
      if (timeElement.isOverlaps(timeLineList.get(i))) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  public Boolean isContained(TimeElement timeElement, List<TimeLine> timeLineList) {
    int i = 0;
    while (i < timeLineList.size()) {
      if (timeElement.isContainedBy(timeLineList.get(i))) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  public Boolean isExOverlapped(TimeElement timeElement, List<TimeLine> timeLineList) {
    int i = 0;
    while (i < timeLineList.size()) {
      if (timeElement.isExOverlaps(timeLineList.get(i))) {
        return true;
      }
      i += 1;
    }
    return false;
  }

  /**
   * 检查XZT单元是否满足包含类型的timeQuery的要求，并相应地处理传入的各项参数.
   * @param timeElement 待检查的XZT单元
   * @param timeQuery 时间查询范围，此处的拓扑约束为包含
   * @param level timeElement的层级，与其序列长度相同（与Bin等长的单元level = 0）
   * @param ranges 当前已有的ranges的引用
   * @param remaining 保存子XZT单元的队列
   * @param timeBin timeElement所属的TimeBin
   * @return 更新后的ranges.
   */
  public List<TimeIndexRange> containSequenceCodeRange(TimeElement timeElement,
                                                       TimeLine timeQuery, short level, List<TimeIndexRange> ranges,
                                                       Deque<TimeElement> remaining, TimeBin timeBin) {
    if (timeElement.isExContainedBy(timeQuery)) {
      TimeIndexRange timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
          false, true);
      ranges.add(timeIndexRange);
    } else if (timeElement.isExOverlaps(timeQuery)) {
      // some portion of this range is excluded
      // add the partial match and queue up each sub-range for processing
      if (canStoreContainedObjects(timeElement, timeQuery)) {
        TimeIndexRange timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
            true, false);
        ranges.add(timeIndexRange);
      }
      remaining.addAll(timeElement.getChildren());
    }
    return ranges;
  }

  /**
   * 检查XZT单元是否满足交叠类型的timeQuery的要求，并相应地处理传入的各项参数.
   * @param timeElement 待检查的XZT单元
   * @param timeQuery 时间查询范围，此处的拓扑约束为交叠
   * @param level timeElement的层级，与其序列长度相同（与Bin等长的单元level = 0）
   * @param ranges 当前已有的ranges的引用
   * @param remaining 保存子XZT单元的队列
   * @param timeBin timeElement所属的TimeBin
   * @return 更新后的ranges.
   */
  public List<TimeIndexRange> overlapSequenceCodeRange(TimeElement timeElement,
                                                       TimeLine timeQuery, short level, List<TimeIndexRange> ranges,
                                                       Deque<TimeElement> remaining, TimeBin timeBin) {
    TimeIndexRange timeIndexRange;
    if (timeElement.isExContainedBy(timeQuery)) {
      timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
          false, true);
      ranges.add(timeIndexRange);
    } else if (timeElement.isExOverlaps(timeQuery)) {
      // 若timeQuery包含了timeElement右端点，则timeElement内的时间范围必与timeQuery相交。
      if (timeQuery.getReTimeStart() <= timeElement.getTimeEnd() && timeQuery.getReTimeEnd() >= timeElement.getTimeEnd()) {
        timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
            true, true);
      } else {
        timeIndexRange = sequenceInterval(timeElement.getTimeStart(), level, timeBin,
            true, false);
      }
      ranges.add(timeIndexRange);
      remaining.addAll(timeElement.getChildren());
    }
    return ranges;
  }

  /**
   * 将时间查询范围规范化至TimeBin内。
   * @param timeLine
   * @param timeBin
   * @return
   */
  public TimeLine queryRefBin(TimeLine timeLine, TimeBin timeBin) {
    double[] doubles = normalize(timeLine.getTimeStart(), timeLine.getTimeEnd(), timeBin, false);
    return new TimeLine(doubles[0], doubles[1]);
  }

  public List<TimeIndexRange> getMaxLevelSequenceCodeRange(short level, TimeLine timeQuery,
      List<TimeIndexRange> ranges, Deque<TimeElement> remaining, TimeBin timeBin) {
    // bottom out and get all the ranges that partially overlapped but we didn't fully process
    while (!remaining.isEmpty()) {
      TimeElement poll = remaining.poll();
      if (poll.equals(levelSeparator)) {
        level = (short) (level + 1);
      } else {
        if (poll.isExContainedBy(timeQuery)) {
          TimeIndexRange timeIndexRange = sequenceInterval(poll.getTimeStart(), level, timeBin,
              false, true);
          ranges.add(timeIndexRange);
        } else if (poll.isExOverlaps(timeQuery)) {
          TimeIndexRange timeIndexRange = sequenceInterval(poll.getTimeStart(), level, timeBin,
              false, false);
          ranges.add(timeIndexRange);
        }
      }
    }
    return ranges;
  }

  public List<TimeIndexRange> mergeCodeRange(List<TimeIndexRange> ranges) {
    ranges.sort(Comparator.comparing(TimeIndexRange::getLowerXZTCode));
    List<TimeIndexRange> result = new ArrayList<>();
    if (ranges.size() == 0) {
      return result;
    }
    TimeIndexRange current = ranges.get(0);
    int i = 1;
    while (i < ranges.size()) {
      TimeIndexRange indexRange = ranges.get(i);
      if (indexRange.getLowerXZTCode() == current.getUpperXZTCode() + 1
          & indexRange.isContained() == current.isContained()) {
        // merge the two ranges
        current = new TimeIndexRange(current.getLowerXZTCode(),
            Math.max(current.getUpperXZTCode(), indexRange.getUpperXZTCode()), indexRange.isContained());
      } else {
        // append the last range and set the current range for future merging
        result.add(current);
        current = indexRange;
      }
      i += 1;
    }
    result.add(current);
    return result;
  }

  /**
   * 获取TimeElement相关的编码区间，可能长度为1（仅包含自己），或包含以自己为前缀的全部XZT单元。
   * @param timeStart TimeElement的起始时间戳
   * @param level TimeElement的层级，和timestart一起用于获取TimeElement的SequenceCode
   * @param timeBin TimeElement所属的时间桶
   * @param singleRange 生成的TimeIndexRange长度是否应为1
   * @param isValidateRange 生成的Range中是否全部满足查询条件
   * @return
   */
  public TimeIndexRange sequenceInterval(double timeStart, short level, TimeBin timeBin,
      Boolean singleRange, Boolean isValidateRange) {
    long min = sequenceCode(timeStart, level);
    long max;
    if (singleRange) {
      max = min;
    } else {
      // 获取以该min所代表的XZT单元为前缀的，字典序最大的XZT单元的编码。
      max = min + (long) (Math.pow(2, g - level + 1) - 1L) - 1;
    }
    return new TimeIndexRange(getXZTCode(timeBin, min), getXZTCode(timeBin, max), isValidateRange);
  }

  public long getXZTCode(TimeBin timeBin, long elementCode) {
    return (long) timeBin.getBinID() * binElementCnt() + elementCode;
  }

  /**
   * 获取查询区间覆盖涉及的时间桶。
   * @param timeQuery 时间查询范围
   * @param isContainQuery 时间查询类型，若为包含查询则为真。
   * @return 特定类型的时间查询范围涉及的时间桶。
   */
  public List<TimeBin> getTimeBinList(TimeLine timeQuery, boolean isContainQuery) {
    short binIdStart = (short) timePeriod.getChronoUnit().between(Epoch, timeQuery.getTimeStart());
    short binIDEnd = (short) timePeriod.getChronoUnit().between(Epoch, timeQuery.getTimeEnd());
    List<TimeBin> timeBins = new ArrayList<>();
    // 若为交叠查询，则需要从timeQuery.start所属的bin的前一个开始搜索。
    int i = isContainQuery ? binIdStart : binIdStart - 1;
    for (; i <= binIDEnd; i++) {
      TimeBin bin = new TimeBin((short) i, timePeriod);
      timeBins.add(bin);
    }
    return timeBins;
  }

  /**
   * 检查TimeElement是否有可能管理了完全包含于timeQuery的时间范围。
   * 依据：
   * 当element层级l < g.
   * <li>
   *   <item>timeQuery应跨element扩展单元的两个半区</item>
   *   <item>timeQuery应穿过两条以上l+1层的时段分割线</item>
   * </li>
   * @param element
   * @param timeQuery
   * @return
   */
  private boolean canStoreContainedObjects(TimeElement element, TimeLine timeQuery) {
    if (element.getTimeEnd() > timeQuery.getReTimeEnd() && element.getTimeEnd() < timeQuery.getReTimeEnd()) {
      TimeElement extElement = element.getExtElement();
      TimeLine extOverlappedTimeLine = extElement.getExtOverlappedTimeLine(timeQuery);
      double finnerElementLen = element.getLength() / 2;
      // 计算穿过线的条数
      // 比start大的最小整数
      int lowerLineID = (int) (extOverlappedTimeLine.getReTimeStart() / finnerElementLen) + 1;
      // 比end小的最大整数
      int upperLineID = (int) (extOverlappedTimeLine.getReTimeEnd() / finnerElementLen)
          - extOverlappedTimeLine.getReTimeEnd() % finnerElementLen == 0 ? 1 : 0;
      return upperLineID - lowerLineID >= 1;
    } else {
      return false;
    }
    // if (element.isContainedBy(timeQuery) || element.isOverlaps(timeQuery)) {
    //   TimeElement extElement = element.getExtElement();
    //   // condition 2
    //   if (extElement.isContainedBy(timeQuery) || extElement.isOverlaps(timeQuery)) {
    //     // condition 3
    //     TimeLine extOverlappedTimeLine = extElement.getExtOverlappedTimeLine(timeQuery);
    //     return extOverlappedTimeLine.getReTimeEnd() - extOverlappedTimeLine.getReTimeStart()
    //         > element.getLength() / 2;
    //   }
    // }
    // return false;
  }

  /**
   * 以timeBin为参照，将时间范围归一化。
   * 对于轨迹的时间范围，应保证其结束时间小于ext bin所覆盖的最大时间。
   * 对于查询的时间范围，其时间范围没有限制。
   * @param startTime 被规范化的时间范围的起点
   * @param endTime 被规范化的时间范围的终点
   * @param timeBin 作为参照的TimeBin
   * @param extBinContained 被规范化的时间范围是否必须包含于ext bin内部。
   * @return 轨迹时间范围规范化后应在区间[0,2]内部。查询时间范围的第一项可以小于0，第二项可以大于2。
   */
  public double[] normalize(ZonedDateTime startTime, ZonedDateTime endTime, TimeBin timeBin,
      Boolean extBinContained) {
    double nStart = (timeBin.getRefTime(startTime) * 1.0) / timePeriod.getChronoUnit().getDuration()
        .getSeconds();
    double nEnd = 0.0;
    if (extBinContained) {
      if (timePeriod.getChronoUnit().getDuration()
          .compareTo(Duration.ofSeconds(timeBin.getRefTime(endTime) / 2)) > 0) {
        nEnd = timeBin.getRefTime(endTime) * 1.0 / timePeriod.getChronoUnit().getDuration()
            .getSeconds();
      } else {
        LOGGER.error(
            "The timeBin granules are too small to accommodate this length of time,please adopt a larger time granularity");
        throw new IllegalArgumentException();
      }
    } else {
      nEnd =
          timeBin.getRefTime(endTime) * 1.0 / timePeriod.getChronoUnit().getDuration().getSeconds();
    }
    return new double[]{nStart, nEnd};
  }

  public Boolean predicate(double min, double max, double length) {
    return max <= (Math.floor(min / length) * length) + (2 * length);
  }

  public long sequenceCode(double timeStart, int level) {
    double timeMin = 0.0;
    double timeMax = 1.0;
    long indexCode = 0L;
    int i = 0;
    while (i < level) {
      double timeCenter = (timeMin + timeMax) / 2.0;
      if (timeStart - timeCenter < 0) {
        indexCode += 1L;
        timeMax = timeCenter;
      } else {
        indexCode += 1L + (long) (Math.pow(2, g - i) - 1L);
        timeMin = timeCenter;
      }
      i += 1;
    }
    return indexCode;
  }
}

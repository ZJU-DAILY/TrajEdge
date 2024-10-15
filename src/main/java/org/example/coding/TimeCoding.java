package org.example.coding;

import org.example.datatypes.TimeBin;
import org.example.datatypes.TimeLine;
import org.example.datatypes.TimePeriod;
import org.example.query.condition.TemporalQueryCondition;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * @author Haocheng Wang Created on 2022/10/4
 */
public interface TimeCoding extends Serializable {

  long getIndex(TimeLine timeline);

  TimeBin dateToBinnedTime(ZonedDateTime zonedDateTime);

  TimeLine getXZTElementTimeLine(long coding);

  TimePeriod getTimePeriod();

  long getIndex(ZonedDateTime start, ZonedDateTime end);

  List<CodingRange> ranges(TemporalQueryCondition condition);
}

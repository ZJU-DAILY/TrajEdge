package org.example.constant;

import org.example.datatypes.TimePeriod;

/**
 * @author Haocheng Wang
 * Created on 2022/9/27
 */
public class CodingConstants {
  /**
   * Max length of xz2 Quadrant sequence
   */
  public static final short MAX_XZ2_PRECISION = 2;

  public static final double XZ2_X_MIN = -180.0;
  public static final double XZ2_X_MAX = 180.0;
  public static final double XZ2_Y_MIN = -90.0;
  public static final double XZ2_Y_MAX = 90.0;

  // default is 7
  public static final short MAX_TIME_BIN_PRECISION = 2;

  public static final TimePeriod DEFAULT_TIME_PERIOD = TimePeriod.DAY;
  public static final double LOG_FIVE = Math.log(0.5);
  public static final int MAX_OID_LENGTH = 60;
  public static final int MAX_TID_LENGTH = 60;

  public static final short XZ2PLUS_POS_CELL_LEN = 2;
}

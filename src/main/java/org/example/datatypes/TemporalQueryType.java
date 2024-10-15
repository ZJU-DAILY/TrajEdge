package org.example.datatypes;

/**
 * @author Xu Qi
 * @since 2022/10/12
 */
public enum TemporalQueryType {
  /**
   * Query all data that may contained with query window.
   */
  CONTAIN,
  /**
   * Query all data that is totally INTERSECT in query window.
   */
  INTERSECT;
}

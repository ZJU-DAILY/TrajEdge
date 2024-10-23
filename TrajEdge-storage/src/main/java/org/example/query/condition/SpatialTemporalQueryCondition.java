package org.example.query.condition;

/**
 * @author Xu Qi
 * @since 2022/11/30
 */
public class SpatialTemporalQueryCondition {

  private final SpatialQueryCondition spatialQueryCondition;
  private final TemporalQueryCondition temporalQueryCondition;

  public SpatialTemporalQueryCondition(SpatialQueryCondition spatialQueryCondition,
      TemporalQueryCondition temporalQueryCondition) {
    this.spatialQueryCondition = spatialQueryCondition;
    this.temporalQueryCondition = temporalQueryCondition;
  }

  public SpatialQueryCondition getSpatialQueryCondition() {
    return spatialQueryCondition;
  }

  public TemporalQueryCondition getTemporalQueryCondition() {
    return temporalQueryCondition;
  }

  @Override
  public String toString() {
    return "SpatialTemporalQueryCondition{" +
        "spatialQueryCondition=" + spatialQueryCondition +
        ", temporalQueryCondition=" + temporalQueryCondition +
        '}';
  }
}

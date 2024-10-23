package org.example.query.condition;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

/**
 * 空间查询条件
 *
 * @author Haocheng Wang
 * Created on 2022/9/27
 */
public class SpatialQueryCondition extends AbstractQueryCondition{
  /**
   * Spatial query window geometry, may be geometry collection
   */
  private Envelope queryWindow;
  private Geometry geometryWindow;

  private SpatialQueryType queryType;

  public SpatialQueryCondition(Geometry geometryWindow, SpatialQueryType queryType) {
    this.geometryWindow = geometryWindow;
    this.queryWindow = geometryWindow.getEnvelopeInternal();
    this.queryType = queryType;
  }

  public Envelope getQueryWindow() {
    return queryWindow;
  }

  public String getQueryWindowWKT() {
    WKTWriter writer = new WKTWriter();
    return writer.write(geometryWindow);
  }

  public Geometry getGeometryWindow() {
    return geometryWindow;
  }

  public void setGeometryWindow(Geometry geometryWindow) {
    this.geometryWindow = geometryWindow;
  }

  public void setQueryWindow(Envelope queryWindow) {
    this.queryWindow = queryWindow;
  }

  public SpatialQueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(SpatialQueryType queryType) {
    this.queryType = queryType;
  }

  @Override
  public String getConditionInfo() {
     return "SpatialQueryCondition{" +
        "queryWindow=" + queryWindow +
        ", queryType=" + queryType +
        '}';
  }

  /**
   * @author Haocheng Wang
   * Created on 2022/9/27
   *
   * 将查询窗口用于什么样的查询: 两类: 严格包含查询\相交包含查询
   */
  public enum SpatialQueryType {
    /**
     * Query all data that may INTERSECT with query window.
     */
    CONTAIN,
    /**
     * Query all data that is totally contained in query window.
     */
    INTERSECT;
  }
}

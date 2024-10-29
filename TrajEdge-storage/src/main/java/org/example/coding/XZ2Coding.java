package org.example.coding;

import org.example.coding.sfc.SFCRange;
import org.example.coding.sfc.XZ2SFC;
import org.example.constant.CodingConstants;
import org.example.datatypes.ByteArray;
import org.example.query.condition.SpatialQueryCondition;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 *  包装XZ2SFC, 负责 <br>
 *  1. 接收一个轨迹/bb,输出xz2 code. <br>
 *  2. 接收一个空间范围 + 查询条件, 输出intervals
 *
 * @author Haocheng Wang
 * Created on 2022/9/26
 */
public class XZ2Coding implements SpatialCoding {
  
  private static final Logger logger = LoggerFactory.getLogger(XZ2Coding.class);

  public static final int BYTES = Long.BYTES;

  private XZ2SFC xz2Sfc;

  short xz2Precision;

  public XZ2Coding() {
    xz2Precision = CodingConstants.MAX_XZ2_PRECISION;
    xz2Sfc = XZ2SFC.getInstance(xz2Precision);
  }

  public XZ2SFC getXz2Sfc() {
    return xz2Sfc;
  }

  /**
   * Get xz2 index for the line string.
   *
   * @param lineString Line string to be indexed.
   * @return The XZ2 code
   */
  public ByteArray code(LineString lineString) {
    Envelope boundingBox = lineString.getEnvelopeInternal();
    double minLng = boundingBox.getMinX();
    double maxLng = boundingBox.getMaxX();
    double minLat = boundingBox.getMinY();
    double maxLat = boundingBox.getMaxY();
    // lenient is false so the points out of boundary can throw exception.
    ByteBuffer br = ByteBuffer.allocate(Long.BYTES);
    br.putLong(xz2Sfc.index(minLng, maxLng, minLat, maxLat, false));
    return new ByteArray(br);
  }

  public ByteArray rawCode(double minLng, double maxLng, double minLat, double maxLat) {
    // lenient is false so the points out of boundary can throw exception.
    ByteBuffer br = ByteBuffer.allocate(Long.BYTES);
    br.putLong(xz2Sfc.index(minLng, maxLng, minLat, maxLat, false));
    return new ByteArray(br);
  }

  /**
   * Get index ranges of the query range, support two spatial query types
   * @param spatialQueryCondition Spatial query on the index.
   * @return List of xz2 index ranges corresponding to the query range.
   */
  public List<CodingRange> ranges(SpatialQueryCondition spatialQueryCondition) {
    Envelope envelope = spatialQueryCondition.getQueryWindow();
    List<CodingRange> codingRangeList = new LinkedList<>();
    List<SFCRange> sfcRangeList = xz2Sfc.ranges(envelope, spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN);
    for (SFCRange sfcRange : sfcRangeList) {
      CodingRange codingRange = new CodingRange();
      codingRange.concatSfcRange(sfcRange);
      codingRangeList.add(codingRange);
    }
    return codingRangeList;
  }

  public List<CodingRange> rawRanges(double xmin, double ymin, double xmax, double ymax) {
    List<SFCRange> sfcRangeList = xz2Sfc.ranges(xmin, ymin, xmax, ymax, false);
    List<CodingRange> codingRangeList = new LinkedList<>();
    for (SFCRange sfcRange : sfcRangeList) {
      CodingRange codingRange = new CodingRange();
      codingRange.concatSfcRange(sfcRange);
      codingRangeList.add(codingRange);
    }
    return codingRangeList;
  }

  @Override
  public Polygon getCodingPolygon(ByteArray spatialCodingByteArray) {
    ByteBuffer br = spatialCodingByteArray.toByteBuffer();
    ((Buffer) br).flip();
    long coding = br.getLong();
    return xz2Sfc.getEnlargedRegion(coding);
  }

  public String getRawCode(ByteArray spatialCodingByteArray) {
    ByteBuffer br = spatialCodingByteArray.toByteBuffer();
    ((Buffer) br).flip();
    long coding = br.getLong();
    List<Integer> res = xz2Sfc.getQuadrantSequence(coding);
    StringBuilder rawCode = new StringBuilder();
    rawCode.append("#");
    for(int i = 0; i < xz2Precision; i++){
      if(i < res.size())rawCode.append(res.get(i));
      else break;
    }
    return rawCode.toString();
  }

  @Override
  public String toString() {
    return "XZ2Coding{" +
        "xz2Precision=" + xz2Precision +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    XZ2Coding xz2Coding = (XZ2Coding) o;
    return xz2Precision == xz2Coding.xz2Precision;
  }

  @Override
  public int hashCode() {
    return Objects.hash(xz2Precision);
  }
}

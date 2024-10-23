package org.example.coding;

import org.example.coding.poscode.PosCode;
import org.example.coding.poscode.PosCodeRange;
import org.example.coding.poscode.QuadID;
import org.example.coding.sfc.SFCRange;
import org.example.coding.sfc.XZ2SFC;
import org.example.constant.CodingConstants;
import org.example.datatypes.ByteArray;
import org.example.query.condition.SpatialQueryCondition;
import org.locationtech.jts.geom.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * xz2(8 Bytes) + Pos Code(1 Byte)
 *
 * @author Haocheng Wang
 * Created on 2022/11/2
 */
public class XZ2PCoding implements SpatialCoding {

  private static final Logger logger = LoggerFactory.getLogger(XZ2PCoding.class);

  public static final int BYTES = Long.BYTES + 1;

  private XZ2SFC xz2Sfc;

  public XZ2PCoding() {
    this.xz2Sfc = XZ2SFC.getInstance(CodingConstants.MAX_XZ2_PRECISION);
  }

  public XZ2SFC getXz2Sfc() {
    return xz2Sfc;
  }

  public ByteArray code(LineString lineString) {
    Envelope boundingBox = lineString.getEnvelopeInternal();
    double minLng = boundingBox.getMinX();
    double maxLng = boundingBox.getMaxX();
    double minLat = boundingBox.getMinY();
    double maxLat = boundingBox.getMaxY();
    // lenient is false so the points out of boundary can throw exception.
    long xz2Value = xz2Sfc.index(minLng, maxLng, minLat, maxLat, false);
    PosCode posCode = new PosCode(xz2Value, lineString);
    return concat(xz2Value, posCode);
  }

  /**
   * 串接xz2 code sequence与pos code, XZ2Value在前, pos code在后.
   * @param xz2Value
   * @param posCode
   * @return
   */
  private ByteArray concat(long xz2Value, PosCode posCode) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + PosCode.SIZE / Byte.SIZE);
    byteBuffer.putLong(xz2Value);
    byteBuffer.put(posCode.getPoscodeByte());
    return new ByteArray(byteBuffer);
  }

  public List<CodingRange> ranges(SpatialQueryCondition spatialQueryCondition) {
    List<CodingRange> res = new ArrayList<>(100);
    Envelope envelope = spatialQueryCondition.getQueryWindow();
    boolean isSpatialContainQuery = spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN;
    List<SFCRange> rawSFCRange = xz2Sfc.ranges(envelope, isSpatialContainQuery);
    // 以上为xz2的索引范围, 接下来需要串接PosCode
    for (SFCRange sfcRange : rawSFCRange) {
      // contained, 该Index range的所有轨迹都位于查询范围内部
      if (sfcRange.validated) {
        CodingRange codingRange = new CodingRange();
        codingRange.concatSfcRange(sfcRange);
        res.add(codingRange);
      } else {
        // not contained, 该Index range内所有的xz2 code的ext range与查询范围交叉
        for (long i = sfcRange.lower; i <= sfcRange.upper; i++) {
          Set<QuadID> quads = getIntersectedQuadIDs(envelope, i);
          List<PosCodeRange> posCodeRanges = PosCode.toPosCodeRanges(PosCode.listPossiblePosCodes(quads, isSpatialContainQuery));
          for (PosCodeRange posCodeRange : posCodeRanges) {
            CodingRange codingRange = new CodingRange();
            codingRange.concatSfcRange(new SFCRange(i, i, false));
            codingRange.concatPosCodeRange(posCodeRange);
            res.add(codingRange);
          }
        }
      }
    }
    return res;
  }

  public Polygon getCodingPolygon(ByteArray xz2pCode) {
    return xz2Sfc.getRegion(extractXZ2CodingVal(xz2pCode));
  }

  private long extractXZ2CodingVal(ByteArray xz2pCode) {
    ByteBuffer br = xz2pCode.toByteBuffer();
    ((Buffer) br).flip();
    return br.getLong();
  }

  /**
   * 获取查询范围与该xz2扩展网格的QuadRegion的QuadID列表
   * @param envelope
   * @param xz2CodeSequence
   * @return
   */
  public Set<QuadID> getIntersectedQuadIDs(Envelope envelope, long xz2CodeSequence) {
    Set<QuadID> res = new HashSet<>();
    Geometry geom = new GeometryFactory().toGeometry(envelope);
    for (int quadID = 0; quadID < 4; quadID++) {
      Polygon quadRegion = xz2Sfc.getQuadRegion(xz2CodeSequence, quadID);
      if (geom.intersects(quadRegion)) {
        res.add(new QuadID(quadID));
      }
    }
    return res;
  }

  public long getXZ2Code(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    return buffer.getLong();
  }

  public PosCode getPosCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getLong();
    return new PosCode(buffer.get());
  }
}

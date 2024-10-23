package org.example.coding.poscode;

import org.example.coding.sfc.XZ2SFC;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.example.constant.CodingConstants;
import java.util.*;

/**
 * pos code记录了一条轨迹在一个xz2 element中存储的位置，由四个{@code QuadID}表达，记录于poscodeByte的低4bit中。 <br>
 * Pos Code的四个QuadID中，LeftBottom(0)、LeftTop(2)二者至少有一个为1，且至少涉及两个QuadID.
 *
 * @author Haocheng Wang
 * Created on 2022/11/7
 */
public class PosCode implements Comparable<PosCode> {

  // Pos Code的字节值取值范围为[5, 7]与[9, 15]
  private byte poscodeByte;
  private final XZ2SFC XZ2 = XZ2SFC.getInstance(CodingConstants.MAX_XZ2_PRECISION);

  public static int SIZE = 8;

  public PosCode() {
    this.poscodeByte = 0;
  }

  /**
   *
   * @param xz2Sequence
   * @param lineString
   * @return pos code about the line string on xz2 region.
   */
  public PosCode(long xz2Sequence, LineString lineString) {
    for (int quadID = 0; quadID < 4; quadID++) {
      Polygon quadRegion = XZ2.getQuadRegion(xz2Sequence, quadID);
      if (lineString.intersects(quadRegion)) {
        this.setQuadID(new QuadID(quadID));
      }
    }
  }

  public byte getPoscodeByte() {
    return poscodeByte;
  }

  public void setQuadID(QuadID quadID) {
    poscodeByte = (byte) (poscodeByte | (1 << (3 - quadID.quadID)));
  }

  public void dropQuadID(QuadID quadID) {
    poscodeByte = (byte) (poscodeByte & ~(1 << (3 - quadID.quadID)));
  }

  public Set<QuadID> getQuadIDSet() {
    Set<QuadID> set = new HashSet<>();
    for (int i = 0; i < 4; i++) {
      if ((poscodeByte >> (3 - i) & 1) == 1) {
        set.add(new QuadID(i));
      }
    }
    return set;
  }


  public PosCode(byte poscodeByte) {
    this.poscodeByte = poscodeByte;
  }

  @Override
  public String toString() {
    return "PosCode{" +
        "poscode=" + Integer.toBinaryString(poscodeByte) +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PosCode posCode = (PosCode) o;
    return poscodeByte == posCode.poscodeByte;
  }

  @Override
  public int hashCode() {
    return Objects.hash(poscodeByte);
  }


  /**
   * 根据某XZ2编码与查询范围相交的QuadID集合与查询要求，获取所有可能符合查询条件的PosCode.
   * @param relatedQuadIDs 某XZ2编码与查询范围相交的QuadID集合
   * @param isContainQuery 是否为包含查询
   * @return 所有可能符合查询条件的PosCode
   */
  public static Set<PosCode> listPossiblePosCodes(Set<QuadID> relatedQuadIDs, boolean isContainQuery) {
    if (isContainQuery) {
      return listPossiblePosCodesContained(relatedQuadIDs);
    } else {
      return listPossiblePosCodesIntersect(relatedQuadIDs);
    }
  }

  /**
   * 包含查询时符合条件的Pos Code应满足以下要求：
   * <ol>
   *     <li>基本要求：至少有两个QuadID，且LB、LT中至少有一个</li>
   *     <li>PosCode中所有的Quad ID均包含于{@param quadIDS}中</li>
   * </ol>
   */
  public static Set<PosCode> listPossiblePosCodesContained(Set<QuadID> relatedQuadIDs) {
    Set<PosCode> set = new HashSet<>();
    if (relatedQuadIDs.size() < 2 || (!relatedQuadIDs.contains(QuadID.LEFT_BOTTOM) && !relatedQuadIDs.contains(QuadID.LEFT_TOP))) {
      return set;
    }
    ArrayList<QuadID> list = new ArrayList<>(relatedQuadIDs);
    Collections.sort(list);
    backtrack(set, 0, list, new PosCode());
    return getValidatedPosCodes(set);
  }

  private static void backtrack(Set<PosCode> res, int offset, List<QuadID> choices, PosCode posCode) {
    if (offset == choices.size()) {
      res.add(new PosCode(posCode.poscodeByte));
      return;
    }
    for (int i = offset; i < choices.size(); i++) {
      posCode.setQuadID(choices.get(i));
      backtrack(res, i+1, choices, posCode);
      posCode.dropQuadID(choices.get(i));
      backtrack(res, i+1, choices, posCode);
    }
  }

  private static Set<PosCode> getValidatedPosCodes(Set<PosCode> set) {
    // 抹去不符合基本要求的
    Set<PosCode> res = new HashSet<>();
    for (PosCode posCode : set) {
      Set<QuadID> quadIDS = posCode.getQuadIDSet();
      if (quadIDS.size() >= 2 && (quadIDS.contains(QuadID.LEFT_BOTTOM) || quadIDS.contains(QuadID.LEFT_TOP))) {
        res.add(posCode);
      }
    }
    return res;
  }

  /**
   * 交叉查询时符合条件的Pos Code应满足以下要求：
   * <ol>
   *     <li>基本要求：至少有两个QuadID，且LB、LT中至少有一个</li>
   *     <li>与{@param quadIDS}存在交集</li>
   * </ol>
   */
  public static Set<PosCode> listPossiblePosCodesIntersect(Set<QuadID> relatedQuadIDs) {
    if (relatedQuadIDs.size() == 0) {
      return new HashSet<>();
    }
    Set<PosCode> possibles = getAllPossiblePosCodes();
    Set<PosCode> res = new HashSet<>();
    for (PosCode possible : possibles) {
      Set<QuadID> quadIDS = possible.getQuadIDSet();
      boolean match = false;
      for (QuadID quadID : relatedQuadIDs) {
        if (quadIDS.contains(quadID)) {
          match = true;
          break;
        }
      }
      if (match) {
        res.add(possible);
      }
    }
    return res;
  }

  private static Set<PosCode> getAllPossiblePosCodes() {
    List<QuadID> choices = QuadID.allQuadIDs();
    Set<PosCode> set = new HashSet<>();
    backtrack(set, 0, choices, new PosCode());
    return getValidatedPosCodes(set);
  }

  /**
   *
   * @param posCodesSet
   * @return Pos code ranges, each range's left and right are included.
   */
  public static List<PosCodeRange> toPosCodeRanges(Set<PosCode> posCodesSet) {
    List<PosCode> posCodes = new ArrayList<>(posCodesSet);
    List<PosCodeRange> ranges = new LinkedList<>();
    if (posCodesSet.isEmpty()) {
      return ranges;
    }
    Collections.sort(posCodes);
    PosCodeRange range = new PosCodeRange();
    for (PosCode posCode : posCodes) {
      byte posCodeByte = posCode.poscodeByte;
      if (range.lower == null) {
        range.lower = new PosCode(posCodeByte);
        range.upper = new PosCode(posCodeByte);
      } else {
        if (range.upper.poscodeByte + 1 == posCodeByte) {
          range.upper = new PosCode(posCodeByte);
        } else {
          ranges.add(range);
          range = new PosCodeRange();
          range.lower = new PosCode(posCodeByte);
          range.upper = new PosCode(posCodeByte);
        }
      }
    }
    ranges.add(range);
    return ranges;
  }

  @Override
  public int compareTo(PosCode o) {
    return this.poscodeByte - o.poscodeByte;
  }
}
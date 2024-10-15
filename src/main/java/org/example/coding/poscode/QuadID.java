package org.example.coding.poscode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author Haocheng Wang
 * Created on 2022/11/7
 */
public class QuadID implements Comparable<QuadID> {

  public static QuadID LEFT_BOTTOM = new QuadID(0);
  public static QuadID LEFT_TOP = new QuadID(1);
  public static QuadID RIGHT_BOTTOM = new QuadID(2);
  public static QuadID RIGHT_TOP = new QuadID(3);

  int quadID;

  public QuadID(int quadID) {
    this.quadID = quadID;
  }

  public static List<QuadID> allQuadIDs() {
    return new ArrayList<>(Arrays.asList(LEFT_BOTTOM, LEFT_TOP, RIGHT_BOTTOM, RIGHT_TOP));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QuadID quadID1 = (QuadID) o;
    return quadID == quadID1.quadID;
  }

  @Override
  public String toString() {
    return "QuadID{" +
        "quadID=" + quadID +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(quadID);
  }

  @Override
  public int compareTo(QuadID quadID) {
    return this.quadID - quadID.quadID;
  }
}

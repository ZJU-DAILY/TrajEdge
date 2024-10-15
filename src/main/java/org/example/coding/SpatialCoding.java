package org.example.coding;

import org.example.datatypes.ByteArray;
import org.example.query.condition.SpatialQueryCondition;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import java.io.Serializable;
import java.util.List;

/**
 * SpatialCoding is a part of the HBase row key, which only contains spatial information of the trajectories storing.
 *
 * @author Haocheng Wang
 * Created on 2022/10/4
 */
public interface SpatialCoding extends Serializable {

  /**
   * Get a spatial code for the trajectory line string.
   *
   * @param lineString Line string represents the point list of a trajectory.
   * @return The spatial coding value calculated by the implemented spatial coding strategy.
   */
  public ByteArray code(LineString lineString);

  /**
   * Get all necessary coding ranges according to the provided query condition. <br>
   * A coarse filtration strategy of the specific coding should be implemented in this function.
   *
   * @return A list of necessary coding ranges satisfying the provided query condition.
   */
  public List<CodingRange> ranges(SpatialQueryCondition spatialQueryCondition);

  /**
   * Get spatial polygon which is represented by the input spatial coding byte array.
   *
   * @param spatialCodingByteArray Spatial coding byte array generated by this coding strategy.
   * @return Polygon represented by the spatial coding.
   * <ul>
   *     <li>for xz2, the polygon is an enlarged range.</li>
   *     <li>for xz2p, which concat xz2 and poscode, the polygon is quad of the enlarged spatial range.</li>
   * </ul>
   */
  public Polygon getCodingPolygon(ByteArray spatialCodingByteArray);
}

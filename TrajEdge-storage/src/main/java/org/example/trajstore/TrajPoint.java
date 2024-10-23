package org.example.trajstore;

import java.io.Serializable;

/**
 * @author alecHe
 * @desc ...
 * @date 2023-11-21 11:09:00
 */
public class TrajPoint implements Serializable {
    Integer trajId;
    Long timestamp;
    Long edgeId;
    Double distance;
    Double oriLat;
    Double oriLng;

    public TrajPoint(Integer trajId, Long timestamp, Long edgeId, Double distance, Double oriLat, Double oriLng) {
        this.trajId = trajId;
        this.timestamp = timestamp;
        this.edgeId = edgeId;
        this.distance = distance;
        this.oriLat = oriLat;
        this.oriLng = oriLng;
    }

    public TrajPoint(TrajPoint other) {
        this.trajId = other.trajId;
        this.timestamp = other.timestamp;
        this.edgeId = other.edgeId;
        this.distance = other.distance;
        this.oriLat = other.oriLat;
        this.oriLng = other.oriLng;
    }

    public Integer getTrajId() {
        return trajId;
    }

    public void setTrajId(Integer trajId) {
        this.trajId = trajId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(Long edgeId) {
        this.edgeId = edgeId;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    public Double getOriLat() {
        return oriLat;
    }

    public void setOriLat(Double oriLat) {
        this.oriLat = oriLat;
    }
    
    public Double getOriLng() {
        return oriLng;
    }

    public void setOriLng(Double oriLng) {
        this.oriLng = oriLng;
    }

    @Override
    public String toString() {
        return "TrajPoint{" +
            "trajId=" + trajId +
            ", timestamp=" + timestamp +
            ", edgeId=" + edgeId +
            ", distance=" + distance +
            ", lat=" + oriLat +
            ", lng=" + oriLng +
            '}';
    }
}

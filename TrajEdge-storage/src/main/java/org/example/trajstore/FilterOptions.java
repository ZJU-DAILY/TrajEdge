package org.example.trajstore;

/**
 * @author alecHe
 * @desc ...
 * @date 2023-11-21 14:03:31
 */

import java.io.Serializable;

/**
 * FilterOptions provides a method to select various filtering options for doing a scan of the metrics database.
 */
public class FilterOptions implements Serializable {
    private long startTime = 0L;
    private long endTime = -1L;
    private Integer trajId;

    public FilterOptions() {
    }

    public Integer getTrajectoryId() {
        return this.trajId;
    }

    public void setTrajectoryId(int trajId) {
        this.trajId = trajId;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(Long time) {
        this.startTime = time;
    }

    /**
     * Returns the end time if set, returns the current time otherwise.
     */
    public long getEndTime() {
        if (this.endTime < 0L) {
            // FIXME: end time bug, it should be a large value
            this.endTime = System.currentTimeMillis();
        }
        return this.endTime;
    }

    public void setEndTime(Long time) {
        this.endTime = time;
    }

}


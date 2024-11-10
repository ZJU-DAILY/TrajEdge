package org.example.struct;

import org.example.coding.CodingRange;
import org.example.coding.XZ2Coding;
import org.example.coding.XZTCoding;
import org.example.datatypes.TimeLine;
import org.example.trajstore.TrajPoint;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.example.datatypes.ByteArray;
import java.time.Instant;

public class STHTIndex {
    private static final Logger LOG = LoggerFactory.getLogger(STHTIndex.class);
    // 常量定义
    // lat
    public static final double X_MIN = 52.9;
    public static final double X_MAX = 53.6;
    // lng
    public static final double Y_MIN = 7.7;
    public static final double Y_MAX = 8.3;
    public static double LogPointFive = Math.log(0.5);


    private XZTCoding xztCoding;
    private XZ2Coding xz2Coding;
    protected double xSize, ySize;

    public STHTIndex() {
        xztCoding = new XZTCoding();
        xz2Coding = new XZ2Coding();
    }

    public String[] encodeTime(long startTime, long endTime) {
        ZonedDateTime stTime = Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault());
        ZonedDateTime enTime = Instant.ofEpochMilli(endTime).atZone(ZoneId.systemDefault());
        TimeLine timeLine = new TimeLine(stTime, enTime);
        String[] res = new String[2];
        ByteArray array = xztCoding.index(timeLine);
        res[0] = String.valueOf(xztCoding.getBinNum(array));
        res[1] = xztCoding.getRawCode(array);

        return res;
    }

    public String encodeSpatial(double minLat, double maxLat, double minLng, double maxLng) {
        return xz2Coding.getRawCode(xz2Coding.rawCode(minLng, maxLng, minLat, maxLat));
    }

    public String encodeUniversalKey(List<TrajPoint> trajectory){
        long startTime = trajectory.get(0).getTimestamp();
        long endTime = trajectory.get(trajectory.size() - 1).getTimestamp();
        double minLat = STHTIndex.X_MAX, maxLat = STHTIndex.X_MIN;
        double minLng = STHTIndex.Y_MAX, maxLng = STHTIndex.Y_MIN;

        for (TrajPoint point : trajectory) {
            minLat = Math.min(minLat, point.getOriLat());
            maxLat = Math.max(maxLat, point.getOriLat());
            minLng = Math.min(minLng, point.getOriLng());
            maxLng = Math.max(maxLng, point.getOriLng());
        }

        String[] timeCode = encodeTime(startTime, endTime);
        String spatialCode = encodeSpatial(minLat, maxLat, minLng, maxLng);
        Key tmp = new Key(timeCode[1], spatialCode);
        return tmp.getKey();
    }

    public List<KeyRange> gKeyRanges(long startTime, long endTime, double minLat, 
    double maxLat, double minLng, double maxLng){
        Set<KeyRange> dedupKeyRanges = new HashSet<>();

        List<CodingRange> sRanges = xz2Coding.rawRanges(minLng, minLat, maxLng, maxLat);
        ZonedDateTime stTime = Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault());
        ZonedDateTime enTime = Instant.ofEpochMilli(endTime).atZone(ZoneId.systemDefault());
        TimeLine timeLine = new TimeLine(stTime, enTime);
        List<CodingRange> tRanges = xztCoding.rawRangesAfterCoding(List.of(timeLine));

        for(CodingRange tRange : tRanges){
            for(CodingRange sRange : sRanges){
                Key lowKey = new Key(xztCoding.getRawCode(tRange.getLower()), xz2Coding.getRawCode(sRange.getLower()));
                Key highKey = new Key(xztCoding.getRawCode(tRange.getUpper()), xz2Coding.getRawCode(sRange.getUpper()));
                dedupKeyRanges.add(new KeyRange(lowKey, highKey));
            }
        }
        return new ArrayList<>(dedupKeyRanges);
    }

}
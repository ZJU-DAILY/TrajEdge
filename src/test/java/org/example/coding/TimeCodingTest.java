package org.example.coding;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.datatypes.TimeLine;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.example.coding.sfc.TimeIndexRange;
import org.example.datatypes.*;


public class TimeCodingTest {
    private static final Logger LOG = LoggerFactory.getLogger(TimeCodingTest.class);
    private XZTCoding xztCoding;

    @Before
    public void setUp() {
        xztCoding = new XZTCoding();
    }

    @Test
    public void testEncodeTime() {
        // 测试时间编码
        ZonedDateTime startTime = ZonedDateTime.of(2021, 01,01,13,0,0,0, ZoneOffset.UTC);
        ZonedDateTime endTime = startTime.plusHours(18);
        TimeLine timeLine = new TimeLine(startTime, endTime);
        long code = xztCoding.getIndex(timeLine);
        ByteArray byteCode = xztCoding.index(timeLine);

        String timeCode = Long.toBinaryString(code);
        
        LOG.info("binary: " + byteCode);
        LOG.info("binary code length: " + byteCode.toString().length());

        LOG.info("encode: " + timeCode);
        LOG.info("code length: " + timeCode.length());
        LOG.info("sequence code: " + xztCoding.getSequenceCode(code));
        LOG.info("bin num: " + xztCoding.getTimeBin(code));
        // 验证时间编码的格式和长度
        assertTrue(timeCode.matches("\\d+[01]{3}"));
    }

    @Test
    public void testQueryTime() {
        // 测试时间编码
        ZonedDateTime startTime = ZonedDateTime.of(2021, 01,01,13,0,0,0, ZoneOffset.UTC);
        ZonedDateTime endTime = startTime.plusHours(18);
        TimeLine timeLine = new TimeLine(startTime, endTime);
        List<TimeIndexRange> ranges = xztCoding.rawRanges(List.of(timeLine));
        for(TimeIndexRange range : ranges) {
            LOG.info("lower sequence code: " + xztCoding.getSequenceCode(range.getLowerXZTCode()));
            LOG.info("lower bin num: " + xztCoding.getTimeBin(range.getLowerXZTCode()));

            LOG.info("uppper sequence code: " + xztCoding.getSequenceCode(range.getUpperXZTCode()));
            LOG.info("upper bin num: " + xztCoding.getTimeBin(range.getUpperXZTCode()));

            // LOG.info(Long.toBinaryString(range.getLowerXZTCode()) + " " + Long.toBinaryString(range.getUpperXZTCode()));
        }

        // List<CodingRange> ranges2 = xztCoding.rawRangesAfterCoding(List.of(timeLine));
        // for(CodingRange range : ranges2) {
        //     LOG.info(range.lower + " " + range.upper);
        // }
    }
}

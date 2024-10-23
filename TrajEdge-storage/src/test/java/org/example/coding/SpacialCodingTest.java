package org.example.coding;
import java.util.List;

import org.example.datatypes.ByteArray;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpacialCodingTest {
    private static final Logger LOG = LoggerFactory.getLogger(SpacialCodingTest.class);
    private XZ2Coding XZ2Coding;

    @Before
    public void setUp() {
        XZ2Coding = new XZ2Coding();
    }

    @Test
    public void testEncodeSpatial() {
        // 测试空间编码
        double minLat = 39.9, maxLat = 40.0;
        double minLng = 116.3, maxLng = 116.4;
        ByteArray code = XZ2Coding.rawCode(minLng, maxLng, minLat, maxLat);
        LOG.info("Encode: "+ code.toString());
        LOG.info("Encode: "+ XZ2Coding.getCodingPolygon(code).toString());
    }

    @Test
    public void testQuerySpatial() {
        // 测试空间编码
        double minLat = 39.8, maxLat = 40.1;
        double minLng = 116.2, maxLng = 116.5;
                List<CodingRange> ranges = XZ2Coding.rawRanges(minLng, minLat, maxLng, maxLat);

        for(CodingRange range : ranges){
            ByteArray lCode = range.getLower();
            LOG.info(lCode.toString());
            LOG.info(XZ2Coding.getCodingPolygon(lCode).toString());

            ByteArray uCode = range.getUpper();
            LOG.info(uCode.toString());
            LOG.info(XZ2Coding.getCodingPolygon(uCode).toString());
        }
    }
}

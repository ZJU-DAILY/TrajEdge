package org.example;

import org.example.grpc.TrajectoryRequest;
import org.example.grpc.TrajectoryResponse;
import org.example.grpc.TrajectoryPoint;
import org.example.grpc.QueryByPrefixRequest;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class NodesServiceTest {
    private NodesService nodesService;
    private static final String TEST_DOCKER_NAME = "localhost";
    private static final int TEST_PORT = 6700;

    @Before
    public void setUp() {
        nodesService = new NodesService(TEST_DOCKER_NAME, TEST_PORT, true);
    }

    @Test
    public void testAddTrajectoryData() {
        // Create test trajectory data
        List<TrajectoryPoint> points = createRealTrajectoryPoints();
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .addAllPoints(points)
                .build();

        // Mock StreamObserver
        @SuppressWarnings("unchecked")
        StreamObserver<TrajectoryResponse> responseObserver = mock(StreamObserver.class);

        // Call the method
        nodesService.addTrajectoryData(request, responseObserver);

        // Verify the response
        ArgumentCaptor<TrajectoryResponse> responseCaptor = ArgumentCaptor.forClass(TrajectoryResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        TrajectoryResponse response = responseCaptor.getValue();
        assertNotNull(response);
        // Add more specific assertions based on expected behavior
    }

    @Test
    public void testReadTrajectoryData() {
        // First insert some test data
        addTestTrajectoryData();

        // Create read request
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .setQueryType(3)
                .setTrajId(-1)
                .setTopk(-1)
                .setStartTime(1000L)
                .setEndTime(2000L)
                .setMinLat(30.0)
                .setMaxLat(40.0)
                .setMinLng(-120.0)
                .setMaxLng(-110.0)
                .build();

        // Mock StreamObserver
        @SuppressWarnings("unchecked")
        StreamObserver<TrajectoryResponse> responseObserver = mock(StreamObserver.class);

        // Call the method
        nodesService.readTrajectoryData(request, responseObserver);

        // Verify the response
        ArgumentCaptor<TrajectoryResponse> responseCaptor = ArgumentCaptor.forClass(TrajectoryResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        TrajectoryResponse response = responseCaptor.getValue();
        assertNotNull(response);
        assertFalse(response.getPointsList().isEmpty());
    }

    @Test
    public void testQueryByPrefix() {
        // First insert some test data
        // addTestTrajectoryData();

        // Create query request
        QueryByPrefixRequest request = QueryByPrefixRequest.newBuilder()
                .setPrefix("#2")
                .setStartTime(1000L)
                .setEndTime(2000L)
                .setMinLat(30.0)
                .setMaxLat(40.0)
                .setMinLng(-120.0)
                .setMaxLng(-110.0)
                .build();

        // Mock StreamObserver
        @SuppressWarnings("unchecked")
        StreamObserver<TrajectoryResponse> responseObserver = mock(StreamObserver.class);

        // Call the method
        nodesService.queryByPrefix(request, responseObserver);

        // Verify the response
        ArgumentCaptor<TrajectoryResponse> responseCaptor = ArgumentCaptor.forClass(TrajectoryResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        TrajectoryResponse response = responseCaptor.getValue();
        assertNotNull(response);
        assertFalse(response.getPointsList().isEmpty());
        // Add more specific assertions based on expected behavior
    }

    @Test
    public void testLongestPrefixMatch() {
        String[] prefixes = {"#2", "#20", "#200", "#201"};
        String key = "#2001";
        String result = NodesService.longestPrefixMatch(key, prefixes);
        assertEquals("#200", result);

        key = "#20100";
        result = NodesService.longestPrefixMatch(key, prefixes);
        assertEquals("#201", result);

        key = "#23";
        result = NodesService.longestPrefixMatch(key, prefixes);
        assertEquals("#2", result);
    }

    private List<TrajectoryPoint> createTestTrajectoryPoints() {
        List<TrajectoryPoint> points = new ArrayList<>();
        // Create test points with different timestamps and locations
        points.add(createPoint(1, 1000L, 1, 0.5, 35.0, -115.0));
        points.add(createPoint(1, 1200L, 2, 1.0, 35.5, -115.5));
        points.add(createPoint(1, 1400L, 3, 1.5, 36.0, -116.0));
        return points;
    }

    private List<TrajectoryPoint> createRealTrajectoryPoints(){
        List<TrajectoryPoint> points = new ArrayList<>();
        points.add(createPoint(1, 1253769789L, 4, 2.0, 39.975515, 116.3328283));
        points.add(createPoint(1, 1253769790L, 5, 2.5, 39.975515, 116.33284));
        points.add(createPoint(1, 1253769792L, 6, 3.0, 39.9755166, 116.3328699));
        points.add(createPoint(1, 1253769794L, 7, 3.5, 39.9755216, 116.3328966));
        points.add(createPoint(1, 1253769796L, 8, 4.0, 39.9755249, 116.3329266));
        points.add(createPoint(1, 1253769798L, 9, 4.5, 39.9755316, 116.3329466));
        points.add(createPoint(1, 1253769800L, 10, 5.0, 39.9755466, 116.3329399));
        points.add(createPoint(1, 1253769802L, 11, 5.5, 39.9755566, 116.3329499));
        points.add(createPoint(1, 1253769804L, 12, 6.0, 39.9755416, 116.3329983));
        points.add(createPoint(1, 1253769806L, 13, 6.5, 39.9755233, 116.3330466));
        return points;
    }

    private TrajectoryPoint createPoint(int trajId, long timestamp, int edgeId, 
            double distance, double lat, double lng) {
        return TrajectoryPoint.newBuilder()
                .setTrajId(trajId)
                .setTimestamp(timestamp)
                .setEdgeId(edgeId)
                .setDistance(distance)
                .setLat(lat)
                .setLng(lng)
                .build();
    }

    private void addTestTrajectoryData() {
        List<TrajectoryPoint> points = createTestTrajectoryPoints();
        TrajectoryRequest request = TrajectoryRequest.newBuilder()
                .addAllPoints(points)
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<TrajectoryResponse> responseObserver = mock(StreamObserver.class);
        nodesService.addTrajectoryData(request, responseObserver);
    }
}

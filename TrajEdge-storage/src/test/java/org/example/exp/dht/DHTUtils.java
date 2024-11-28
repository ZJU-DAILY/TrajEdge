package org.example.exp.dht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;

public class DHTUtils {
    public static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes());
            return DatatypeConverter.printHexBinary(hash).toLowerCase();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    public static String encodeCoordinate(double coord, double min, double max, int bits) {
        double normalized = (coord - min) / (max - min);
        long value = (long) (normalized * ((1L << bits) - 1));
        return String.format("%" + bits + "s", Long.toBinaryString(value)).replace(' ', '0');
    }
} 
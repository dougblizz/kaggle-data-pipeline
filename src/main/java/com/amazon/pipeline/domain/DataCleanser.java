package com.amazon.pipeline.domain;

import java.io.Serializable;

public class DataCleanser implements Serializable {
    public static String formatAmount(String val) {
        if (val == null || val.isEmpty()) return "0.0";

        try {
            double cleanVal = Double.parseDouble(val.replaceAll("[^\\d.]", ""));
            return String.valueOf(cleanVal);
        } catch (Exception e) {
            return "0.0";
        }
    }

    public static String normalizeText(String val) {
        return val != null ? val.trim().toUpperCase() : "";
    }
}

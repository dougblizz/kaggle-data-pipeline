package com.amazon.pipeline.infrastructure.utils;

import java.util.regex.Pattern;

public final class AppConstants {
    private AppConstants() {}

    /**
     * Regex for CSV Splitting:
     * Splits by commas but ignores those within double quotes.
     * Example: 1, "Laptop, 15 inch", 500 -> [1, "Laptop, 15 inch", 500]
     */
    public static final String CSV_SPLIT_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    // Pre-compiled version for maximum performance on Beam
    public static final Pattern CSV_PATTERN = Pattern.compile(CSV_SPLIT_REGEX);
}
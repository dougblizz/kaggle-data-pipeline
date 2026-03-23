package com.amazon.pipeline.domain;

public class SaleValidator {
    public static AmazonSale clean(String[] raw) {
        // clean file
        double amount;
        try {
            amount = Double.parseDouble(raw[15]);
        } catch (Exception e) {
            amount = 0.00;
        }
        return new AmazonSale(raw[1], amount, raw[8]);
    }
}
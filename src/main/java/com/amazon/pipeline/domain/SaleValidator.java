package com.amazon.pipeline.domain;

public class SaleValidator {
    public static AmazonSale clean(String[] raw) {
        double amount;

        try {
            // El monto esta en la columna 15 del CSV original
            amount = Double.parseDouble(raw[15]);
        } catch (Exception e) {
            amount = 0.00;
        }
        // Retorna objeto de dominio limpio: ID (col 1), Amount, Category (col 8)
        return new AmazonSale(raw[1], amount, raw[8]);
    }
}
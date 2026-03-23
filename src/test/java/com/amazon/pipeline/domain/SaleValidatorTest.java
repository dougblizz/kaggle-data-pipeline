package com.amazon.pipeline.domain;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class SaleValidatorTest {

    @Test
    void testSaleValidator() {
        String[] raw = {"1", "ORD123", "2024-01-01", "150.50", "Electronics"};
        AmazonSale result = SaleValidator.clean(raw);
        assertEquals("ORD123", result.id());
        assertEquals(150.50, result.amount());
    }
}

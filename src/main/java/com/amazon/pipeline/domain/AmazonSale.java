package com.amazon.pipeline.domain;
import java.io.Serializable;

public record AmazonSale(String id, double amount, String category) implements Serializable {}
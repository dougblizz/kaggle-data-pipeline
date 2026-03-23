package com.amazon.pipeline.domain;

import java.io.Serializable;

public record FieldMetadata(String name, int index) implements Serializable {}
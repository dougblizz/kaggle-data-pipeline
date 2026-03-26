package com.amazon.pipeline.infrastructure.persistence.mongo;

import com.amazon.pipeline.domain.FieldMetadata;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class MetadataLoader {
    private record ConfigWrapper(List<FieldMetadataDTO> fields) {}
    private record FieldMetadataDTO(String name, int index, Boolean isEntityKey) {}

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static List<FieldMetadata> load(String path) {
        try {
            // NIO for greater efficiency
            ConfigWrapper wrapper = MAPPER.readValue(new File(path), ConfigWrapper.class);

            if (wrapper == null || wrapper.fields() == null) {
                return List.of();
            }

            return wrapper.fields().stream()
                    .map(f -> new FieldMetadata(
                            f.name(),
                            f.index(),
                            Boolean.TRUE.equals(f.isEntityKey())
                    ))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Error processing metadata file in: " + path, e);
        }
    }
}
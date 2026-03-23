package com.amazon.pipeline.infrastructure;

import com.amazon.pipeline.domain.FieldMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.util.List;
import java.util.Map;

public class MetadataLoader {
    public static List<FieldMetadata> load(String path) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            // Leemos el JSON completo
            Map<String, Object> config = mapper.readValue(new File(path), new TypeReference<Map<String, Object>>() {});
            List<Map<String, Object>> fields = (List<Map<String, Object>>) config.get("fields");

            // Mapeamos a nuestro record de dominio
            return fields.stream()
                    .map(f -> new FieldMetadata((String) f.get("name"), (int) f.get("index")))
                    .toList();
        } catch (Exception e) {
            throw new RuntimeException("Error crítico cargando pipeline-metadata.json: " + e.getMessage());
        }
    }
}
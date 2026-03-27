package com.amazon.pipeline.infrastructure.config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class ConfigLoader {
    public static Properties load() {
        Properties prop = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Sorry, I couldn't find application.properties");
            }
            prop.load(input);
        } catch (IOException ex) {
            log.error("IOException: when the properties were loaded, mgs: {} ", ex.getMessage());
        }
        return prop;
    }
}
package com.fraud.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class FlexibleLocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {

    @Override
    public LocalDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String value = p.getText();
        if (value == null || value.trim().isEmpty()) {
            return null;
        }

        // 1. Если пришло число (микросекунды от Debezium, например 1794167697528217)
        if (value.matches("\\d+")) {
            long micros = Long.parseLong(value);
            long seconds = micros / 1_000_000;
            long nanos = (micros % 1_000_000) * 1000;
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneId.systemDefault());
        }

        // 2. Если пришла строка (из curl / REST API, например 2026-11-08T18:43:46.753)
        return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
}

package com.fraud.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraud.model.Transaction;
import com.fraud.model.TransactionMetrics;
import com.fraud.model.DebeziumPayload;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;
import java.time.Duration;

@Configuration
@EnableKafkaStreams
public class KafkaConfig {

    private static final String INPUT_TOPIC = "fraud_db_server.public.transaction";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static final String OUTPUT_TOPIC = "fraud-alerts";

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name("fraud-alerts").partitions(1).replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000").build();
    }

    @Bean
    public NewTopic transactionEventsTopic() {
        return TopicBuilder.name("transaction-events").partitions(1).replicas(1).build();
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder, ObjectMapper objectMapper) {
        JsonSerde<DebeziumPayload> debeziumSerde = new JsonSerde<>(DebeziumPayload.class, objectMapper);
        debeziumSerde.configure(java.util.Collections.emptyMap(), false);

        JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class, objectMapper);
        transactionSerde.configure(java.util.Collections.emptyMap(), false);

        // Добавляем Serde для нашего нового класса метрик
        JsonSerde<TransactionMetrics> metricsSerde = new JsonSerde<>(TransactionMetrics.class, objectMapper);
        metricsSerde.configure(java.util.Collections.emptyMap(), false);

        Serde<String> stringSerde = Serdes.String();

        // 1. Читаем "сырой" поток Debezium
        KStream<String, DebeziumPayload> debeziumStream = builder.stream(
                INPUT_TOPIC,
                Consumed.with(stringSerde, debeziumSerde)
        );

        // 2. Распаковываем "after" в чистый Transaction
        KStream<String, Transaction> stream = debeziumStream
                .filter((key, payload) -> payload != null && payload.getAfter() != null)
                .mapValues(DebeziumPayload::getAfter);

        // 3. Агрегируем одновременно КОЛИЧЕСТВО и СУММУ
        KTable<Windowed<String>, TransactionMetrics> windowedMetrics = stream
                .filter((key, tx) -> tx != null && tx.getStatus() == Transaction.Status.NEW && tx.getUserId() != null)
                .selectKey((key, tx) -> tx.getUserId().toString())
                .groupByKey(Grouped.with(stringSerde, transactionSerde))
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(WINDOW_SIZE, Duration.ofMillis(0)))
                .aggregate(
                        TransactionMetrics::new, // Стартовое состояние (count=0, sum=0.0)
                        (key, tx, aggregate) -> aggregate.increment(tx.getAmount()), // Логика инкремента
                        Materialized.with(null, metricsSerde)
                );

        // ОТЛАДКА: Выводим текущее состояние окна в консоль
        KStream<Windowed<String>, TransactionMetrics> metricsStream = windowedMetrics.toStream();
        metricsStream.peek((windowedKey, metrics) -> {
            System.out.println(">>> [STREAM MONITOR] User: " + windowedKey.key() +
                    " | Tx Count: " + metrics.getCount() +
                    " | Total Amount: " + metrics.getTotalAmount() + " USD");
        });

        // 4. Условие Фрода: Либо КОЛИЧЕСТВО >= 3, Либо СУММА > 1000.0 USD
        metricsStream
                .filter((windowedKey, metrics) -> metrics != null &&
                        (metrics.getCount() >= 3 || metrics.getTotalAmount() > 1000.0))
                .peek((windowedKey, metrics) -> System.out.println(">>> [FRAUD DETECTED TRIPPED] Triggered for user: " + windowedKey.key()))
                .map((windowedKey, metrics) -> new KeyValue<>(windowedKey.key(), windowedKey.key()))
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        return metricsStream.map((winKey, value) -> new KeyValue<>(winKey.key(), String.valueOf(value.getCount())));
    }
}

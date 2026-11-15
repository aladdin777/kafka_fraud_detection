package com.fraud.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;

import java.io.File;

@Profile("!test")
@Configuration
@EnableKafka
public class SslConfig {

    @Value("${user.home}/kafka_fraud_detection/ssl/truststore.jks")
    private String truststoreLocation;

    @Value("${user.home}/kafka_fraud_detection/ssl/kafka.jks")
    private String keystoreLocation;

    @PostConstruct
    public void init() {
        checkFileExists("Truststore", truststoreLocation);
        checkFileExists("Keystore", keystoreLocation);
    }

    private void checkFileExists(String name, String path) {
        File file = new File(path);
        if (!file.exists()) {
            throw new IllegalStateException(name + " file not found: " + path);
        }
        System.out.println(" " + name + " loaded: " + file.getAbsolutePath());
    }
}

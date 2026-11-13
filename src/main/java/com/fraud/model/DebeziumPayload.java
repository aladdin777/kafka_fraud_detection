package com.fraud.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumPayload {

    private Transaction after;

    public Transaction getAfter() {
        return after;
    }

    public void setAfter(Transaction after) {
        this.after = after;
    }
}


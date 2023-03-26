package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    private int seq;

    private final Logger logger = LoggerFactory.getLogger(CustomCallback.class);

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception == null) {
            logger.info("seq: {} partition: {}, offset: {}", seq, metadata.partition(), metadata.offset());
        } else {
            logger.error("exception error from broker = {}", exception.getMessage());
        }
    }
}

package com.practice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.practice.kafka.model.OrderModel;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<OrderModel> {

    private final Logger logger = LoggerFactory.getLogger(OrderDeserializer.class);

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public OrderModel deserialize(String topic, byte[] data) {
        OrderModel orderModel = null;

        try {
            orderModel = objectMapper.readValue(data, OrderModel.class);
        } catch (IOException e) {
            logger.error("Object mapper Deserialization error: " + e.getMessage());
        }

        return orderModel;
    }
}

package com.kschool.kafka;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Esta clase es la encargada de consumir de cada stream de kafka (kafkaStream).
 * Cada mensaje es parseado desde un json string a un mapa (event).
 * El mapa es introducido en la cola (queue) que es utilizada para comunicar el consumidor (Consumer) con el procesador
 * (Processor).
 */
public class ConsumerStream implements Runnable {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    KafkaConsumer<String, String> consumer;
    ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    String topic;

    public ConsumerStream(KafkaConsumer<String, String> consumer, String topic,
                          LinkedBlockingQueue<Map<String, Object>> inQueue) {
        this.inQueue = inQueue;
        this.topic = topic;
        this.consumer = consumer;
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(topic));
        while (!closed.get()) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String kafkaMessage = record.value();

                try {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> event = mapper.readValue(kafkaMessage, Map.class);
                    inQueue.put(event);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}

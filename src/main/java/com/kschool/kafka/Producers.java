package com.kschool.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kschool.Processor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class Producers extends Thread {
    LinkedBlockingQueue<Map<String, Object>> outQueue;
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getProducerConfig());
    ObjectMapper objectMapper;
    String metricTopic;
    String controlTopic;
    String alertTopic;
    String systemName;


    public Producers(String systemName, String controlTopic, String metricTopic, String alertTopic,
                     LinkedBlockingQueue<Map<String, Object>> outQueue) {
        this.outQueue = outQueue;
        this.objectMapper = new ObjectMapper();
        this.metricTopic = metricTopic;
        this.alertTopic = alertTopic;
        this.controlTopic = controlTopic;
        this.systemName = systemName;
    }

    private Properties getProducerConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        return properties;
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            try {
                Map<String, Object> event = outQueue.take();
               // System.out.printf("Type de evento: %s \n", event.get("type"));
               // System.out.printf("systemName:%s \n",systemName);

                event.put("systemName", systemName);

                String eventType = (String) event.get("type");
                String topic = null;

                if (eventType.equals(Processor.EventType.CONTROL.type)) {
                    topic = controlTopic;
                } else if (eventType.equals(Processor.EventType.ALERT.type)) {
                    topic = alertTopic;
                } else if (eventType.equals(Processor.EventType.METRIC.type)) {
                    topic = metricTopic;
                }
                //System.out.printf("Topic al que va: %s \n",topic);
                if (topic != null) {
                    String json = objectMapper.writeValueAsString(event);
                 //   System.out.printf(json);
                    kafkaProducer.send(new ProducerRecord<String, String>(topic,json));
                 //   System.out.println("Enviado");
                } else {
                    System.err.println("The event type is null");
                }

            } catch (InterruptedException e) {
                System.out.println("Apagando el productor ... ");

            } catch (JsonProcessingException e) {
                System.out.println("No se puede convertir a JSON");
            }
        }
    }

    public void shutdown() {
        interrupt();
    }
}

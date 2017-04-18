package com.capitalone.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by krf685 on 7/20/16.
 */
public class KafkaDemoProducer {

    private Producer producer;

    private String topicName = "kafka-demo";

    private void initialize(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("acks","all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(properties);
    }

    private void sendMessages(){

        try {
            for(int i = 0;i < 50; i++){
                producer.send(new ProducerRecord(topicName, "Hello World again " + i));
            }
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        KafkaDemoProducer demoProducer = new KafkaDemoProducer();
        demoProducer.initialize();
        demoProducer.sendMessages();
        if(demoProducer.producer != null) demoProducer.producer.close();
    }
}

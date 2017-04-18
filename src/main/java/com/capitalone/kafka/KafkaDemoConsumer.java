package com.capitalone.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by krf685 on 4/18/17.
 */
public class KafkaDemoConsumer {

    public static void main(String[] args) {
        try {
            Map<String, Object> props = new HashMap<String, Object>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "90000");
            Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Arrays.asList("kafka-demo"));

            while (true){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

                if(consumerRecords.count() > 0){
                    ConsumerRecord<String, String> record = consumerRecords.iterator().next();
                    System.out.println("record.value() = " + record.value());
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}

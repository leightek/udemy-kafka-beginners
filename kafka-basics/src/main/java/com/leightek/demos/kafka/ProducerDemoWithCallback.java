package com.leightek.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to local host
//        properties.setProperty("bootstrap.servers", "SASL_SSL");

        // connect to Conductor Playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"1AbCcIoadCyQ27rFRgZaaH\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxQWJDY0lvYWRDeVEyN3JGUmdaYWFIIiwib3JnYW5pemF0aW9uSWQiOjcxMTEzLCJ1c2VySWQiOjgyMzgwLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJjMTk3YzZkZC1kN2MyLTRhNmYtOWI2OS00NDEyMmFhZWQ2Y2EifX0.kCkvvIQYZ498fcnmFVedz340FRaMunJPy9C8dyJLm8k\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java",
                    "hello world" + i);

            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // executes every time a record successfully sent or exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        // tell the producer to send all data and block util done ... synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}

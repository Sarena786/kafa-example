package io.muzoo.mining.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.zip.ZipInputStream;

public class ReaderFreq {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String FILE_NAME = "/data/auth.log.zip";

        try (InputStream fileStream = ReaderFreq.class.getResourceAsStream(FILE_NAME)) {
            try (ZipInputStream zipStream = new ZipInputStream(fileStream);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(zipStream))) {

                zipStream.getNextEntry();

                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("Sending: " + line);
                    producer.send(new ProducerRecord<>("auth-logs", line));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
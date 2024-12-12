package io.muzoo.mining.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class freq {

    private static final Logger logger = LoggerFactory.getLogger(freq.class);

    private static final String APPLICATION_ID = "atk-count";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String INPUT_TOPIC = "atk-logs";
    private static final String OUTPUT_TOPIC = "atk-freq";

    private static final Pattern IP_PATTERN = Pattern.compile("from (\\d+\\.\\d+\\.\\d+\\.\\d+)");

    public static void main(String[] args) {
        logger.info("Initializing Kafka Streams application...");

        Properties props = createStreamProperties();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC);
        KTable<String, Long> attackCounts = processStream(sourceStream);

        attackCounts.toStream().foreach((ip, count) -> logger.info("IP: {}, Count: {}", ip, count));
        attackCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        startStream(streams);
    }

    private static Properties createStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        logger.info("Kafka Streams properties initialized.");
        return props;
    }


    private static KTable<String, Long> processStream(KStream<String, String> sourceStream) {
        return sourceStream
                .mapValues(Frequencies::extractIp)
                .filter((key, value) -> value != null)
                .groupBy((key, value) -> value)
                .count(Materialized.as("attack-counts"));
    }

    private static String extractIp(String value) {
        try {
            Matcher matcher = IP_PATTERN.matcher(value);
            if (matcher.find()) {
                String ip = matcher.group(1);
                logger.debug("Extracted IP: {}", ip);
                return ip;
            } else {
                logger.warn("No IP address found in log line: {}", value);
  


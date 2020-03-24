package com.feltap.streams;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class StreamColor {
	public static void main(String[] args) {
		List<String> colors = Arrays.asList("RED", "BLUE", "GREEN", "PURPLE");
		String rawTopic = "colors-topic-raw";
		String derivedTopic = "colors-topic-derived";

		Properties streamProperties = new Properties();
		streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.74:9092");
		streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
		streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-colors-app");
		streamProperties.put(StreamsConfig.STATE_DIR_CONFIG,"C:/Users/Felipe/Desktop/state");

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KStream<String, String> streamColors = streamsBuilder.stream(rawTopic,
				Consumed.with(Serdes.String(), Serdes.String()));

		streamColors
				.filter((k, v) -> v != null && v.contains(","))
				.mapValues((k, v) -> v.split(","))
				.filter((k, v) -> v.length == 2)
				.map((k, v) -> new KeyValue<String, String>(v[0].toUpperCase(), v[1].toUpperCase()))
				.filter((k, v) -> colors.contains(v))
				.to(derivedTopic, Produced.with(Serdes.String(), Serdes.String()));

		KTable<String, String> tableColor = streamsBuilder.table(derivedTopic,
				Consumed.with(Serdes.String(), Serdes.String()));

		tableColor.groupBy((k, v) -> new KeyValue<String, String>(v, v), Grouped.with(Serdes.String(), Serdes.String()))
				.count()
				.toStream()
				.peek((k, v) -> System.out.println(String.format("%s -> %d", k, v)));

		final Topology topology = streamsBuilder.build();
		final CountDownLatch latch = new CountDownLatch(1);

		final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamProperties);
		
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
            	kafkaStreams.close();
                latch.countDown();
            }
        });
		
		kafkaStreams.start();

	}
}

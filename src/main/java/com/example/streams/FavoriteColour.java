package com.example.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class FavoriteColour {
    /*
    Topic creation:
    kafka-topics --bootstrap-server localhost:9092 --create --topic fav-colour-input --replication-factor 1 --partitions 3
    kafka-topics --bootstrap-server localhost:9092 --create --topic user-to-fav-colour --replication-factor 1 --partitions 3 --config "cleanup.policy=compact"
    kafka-topics --bootstrap-server localhost:9092 --create --topic colour-count --replication-factor 1 --partitions 3 --config "cleanup.policy=compact"

    Consumer:
    kafka-console-consumer --bootstrap-server localhost:9092 --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true --topic colour-count
     */

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-colour-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> userColourStream = streamsBuilder.stream("fav-colour-input");
        userColourStream
                .selectKey((key, input) -> input.split(",")[0])
                .mapValues(userIdToColour -> userIdToColour.split(",")[1])
                .filter((userId, colour) -> colour.equals("green") || colour.equals("red") || colour.equals("blue"))
                .to("user-to-fav-colour");

        KTable<String, Long> colourCount = streamsBuilder.<String, String>table("user-to-fav-colour")
                .groupBy((userId, colour) -> new KeyValue<>(colour, colour))
                .count();

        colourCount.toStream().to("colour-count");

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();
        System.out.println(topology.describe());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

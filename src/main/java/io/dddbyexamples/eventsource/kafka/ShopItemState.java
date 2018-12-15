package io.dddbyexamples.eventsource.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.dddbyexamples.eventsource.domain.shopitem.ShopItem;
import io.dddbyexamples.eventsource.domain.shopitem.events.DomainEvent;

@Configuration
class ShopItemState {

    @Autowired
    JsonSerdes jsonSerdes;

    @Bean
    public KafkaStreams startShopItemState() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example-5");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Materialized<UUID, ShopItem, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<UUID, ShopItem> as(Stores
                .inMemoryKeyValueStore("shop-items-store"))
                .withKeySerde(jsonSerdes.forA(UUID.class))
                .withValueSerde(jsonSerdes.forA(ShopItem.class));

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<UUID, DomainEvent> shopItems = builder.stream("shop-items", Consumed.with(jsonSerdes.forA(UUID.class), jsonSerdes.forA(DomainEvent.class)));
        KTable<UUID, ShopItem> kTable = shopItems.groupBy((k, v) -> k, Serialized.with(jsonSerdes.forA(UUID.class), jsonSerdes.forA(DomainEvent.class)))
                                                 .aggregate(() -> ShopItem.from(null, Collections
                                                                 .emptyList()),
                                                         (key, value, aggregate) -> aggregate
                                                                 .apply(value),
                                                         materialized);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();

        return kafkaStreams;
    }

}

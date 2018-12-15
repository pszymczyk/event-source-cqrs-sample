package io.dddbyexamples.eventsource.boundary;

import io.dddbyexamples.eventsource.domain.shopitem.ShopItem;
import io.dddbyexamples.eventsource.domain.shopitem.ShopItemRepository;
import io.dddbyexamples.eventsource.domain.shopitem.commands.Buy;
import io.dddbyexamples.eventsource.domain.shopitem.commands.Command;
import io.dddbyexamples.eventsource.domain.shopitem.commands.MarkPaymentTimeout;
import io.dddbyexamples.eventsource.domain.shopitem.commands.Pay;
import io.dddbyexamples.eventsource.domain.shopitem.events.DomainEvent;
import io.dddbyexamples.eventsource.kafka.Futures;
import io.dddbyexamples.eventsource.kafka.JsonSerdes;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;

import com.google.common.collect.ImmutableList;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

@Service
@Transactional
@Slf4j
public class ShopItems {

    private final ShopItemRepository itemRepository;
    private JsonSerdes jsonSerdes;
    private KafkaStreams kafkaStreams;
    private Futures futures;

    private final KafkaProducer<UUID, Command> commandProducer;

    @Autowired
    public ShopItems(ShopItemRepository itemRepository,
            JsonSerdes jsonSerdes,
            KafkaStreams kafkaStreams,
            Futures futures) {
        this.itemRepository = itemRepository;
        this.jsonSerdes = jsonSerdes;
        this.kafkaStreams = kafkaStreams;
        this.futures = futures;
        this.commandProducer = createCommandProducer();
    }

    private KafkaProducer<UUID, Command> createCommandProducer() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "my-client-id");

        return new KafkaProducer<>(producerConfig, jsonSerdes.forA(UUID.class).serializer(),
                jsonSerdes.forA(Command.class).serializer());
    }

    public void buy(Buy command) {
        commandProducer.send(new ProducerRecord<>("shop-items-commands", command.getUuid(), command));
        CompletableFuture<String> completableFuture = futures.add(command.getUuid());

        //await until proceed or throw exception
        try {
            String completed = completableFuture.get(20, TimeUnit.SECONDS);
            System.err.println(completed);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException();
        }
    }

    public void pay(Pay command) {
    }

    public void markPaymentTimeout(MarkPaymentTimeout command) {

    }

    public ShopItem getByUUID(UUID uuid) {
        return Optional.ofNullable(kafkaStreams.store("shop-items-store", QueryableStoreTypes.<UUID, ShopItem> keyValueStore())
                                               .get(uuid)).orElse(ShopItem.from(uuid, Collections.emptyList()));
    }

    private ShopItem withItem(UUID uuid, UnaryOperator<ShopItem> action) {
        final ShopItem tx = getByUUID(uuid);
        final ShopItem modified = action.apply(tx);
        return itemRepository.save(modified);
    }

}

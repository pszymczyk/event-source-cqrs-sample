package io.dddbyexamples.eventsource.kafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.dddbyexamples.eventsource.domain.shopitem.ShopItem;
import io.dddbyexamples.eventsource.domain.shopitem.commands.Buy;
import io.dddbyexamples.eventsource.domain.shopitem.commands.Command;
import io.dddbyexamples.eventsource.domain.shopitem.events.DomainEvent;

import static java.util.Collections.singleton;

@Component
class CommandsProcessor {

    private static final String SOME_GROUP_ID_2 = "some-group-id-5";

    private final Futures futures;
    private final KafkaStreams kafkaStreams;
    private final int hoursToPaymentTimeout;
    private final KafkaConsumer<UUID, Command> consumer;
    private final KafkaProducer<UUID, DomainEvent> producer;

    @Autowired
    public CommandsProcessor(JsonSerdes jsonSerdes,
            KafkaStreams kafkaStreams,
            Futures futures,
            @Value("${hours.to.payment.timeout:48}") int hoursToPaymentTimeout) {
        consumer = createConsumer(jsonSerdes);
        producer = createProducer(jsonSerdes);
        this.kafkaStreams = kafkaStreams;
        this.futures = futures;
        this.hoursToPaymentTimeout = hoursToPaymentTimeout;
        producer.initTransactions();
    }

    private KafkaConsumer<UUID, Command> createConsumer(JsonSerdes jsonSerdes) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, SOME_GROUP_ID_2);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "order-details-service-consumer");

        return new KafkaConsumer<>(consumerConfig, jsonSerdes.forA(UUID.class).deserializer(),
                jsonSerdes.forA(Command.class).deserializer());
    }

    private KafkaProducer<UUID, DomainEvent> createProducer(JsonSerdes jsonSerdes) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "my-client-id");

        return new KafkaProducer<>(producerConfig, jsonSerdes.forA(UUID.class)
                                                             .serializer(), jsonSerdes.forA(DomainEvent.class)
                                                                                      .serializer());
    }

    @PostConstruct
    public void run() {
        consumer.subscribe(singleton("shop-items-commands"));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while (true) {
                try {
                    applyCommandsOnAggregates();
                } catch (Exception ex) {
                    System.err.println(ex);
                    producer.abortTransaction();
                }

            }
        });
    }

    private void applyCommandsOnAggregates() {
        Map<TopicPartition, OffsetAndMetadata> consumedOffsets = new HashMap<>();
        producer.beginTransaction();
        ConsumerRecords<UUID, Command> records = consumer.poll(Long.MAX_VALUE);
        List<UUID> futureIds = new ArrayList<>();

        for (ConsumerRecord<UUID, Command> record : records) {
            if (record.value() instanceof Buy) {
                ShopItem shopItem = applyCommandOnAggregate(record);
                if (commandAppliedCorrectly(shopItem)) {
                    publishUncommittedChangesToKafka(futureIds, record, shopItem);
                }
                recordOffset(consumedOffsets, record);
            }
        }

        producer.sendOffsetsToTransaction(consumedOffsets, SOME_GROUP_ID_2);
        producer.commitTransaction();
        notifyListeners(futureIds);
    }

    private void notifyListeners(List<UUID> futureIds) {
        futureIds.forEach(f -> futures.complete(f));
    }

    private void publishUncommittedChangesToKafka(List<UUID> futureIds, ConsumerRecord<UUID, Command> record, ShopItem shopItem) {
        shopItem.getUncommittedChanges()
                .forEach(domainEvent -> producer.send(new ProducerRecord<>("shop-items", domainEvent.uuid(), domainEvent)));
        futureIds.add(record.key());
    }

    private boolean commandAppliedCorrectly(ShopItem shopItem) {
        return shopItem != null;
    }

    private ShopItem applyCommandOnAggregate(ConsumerRecord<UUID, Command> record) {
        ShopItem shopItem;
        try {
            shopItem = getByUUID(record.key());
            UUID uuid = ((Buy) record.value()).getUuid();
            Instant when = ((Buy) record.value()).getWhen();
            shopItem = shopItem.buy(uuid, when, hoursToPaymentTimeout);
        } catch (Exception e) {
            shopItem = null;
        }
        return shopItem;
    }

    private void recordOffset(Map<TopicPartition, OffsetAndMetadata> consumedOffsets,
            ConsumerRecord<UUID, Command> record) {
        OffsetAndMetadata nextOffset = new OffsetAndMetadata(record.offset() + 1);
        consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), nextOffset);
    }

    private ShopItem getByUUID(UUID key) {
        return Optional.ofNullable(kafkaStreams.store("shop-items-store", QueryableStoreTypes.<UUID, ShopItem> keyValueStore())
                                               .get(key))
                       .orElse(ShopItem.from(key, Collections.emptyList()));
    }

}

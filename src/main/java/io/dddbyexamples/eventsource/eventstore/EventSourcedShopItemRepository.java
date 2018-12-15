package io.dddbyexamples.eventsource.eventstore;


import io.dddbyexamples.eventsource.domain.shopitem.ShopItem;
import io.dddbyexamples.eventsource.domain.shopitem.ShopItemRepository;
import io.dddbyexamples.eventsource.domain.shopitem.events.DomainEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

@Component
public class EventSourcedShopItemRepository implements ShopItemRepository {

    private final EventStore eventStore;
    private final EventSerializer eventSerializer;
    private final ApplicationEventPublisher eventPublisher;



    @Autowired
    public EventSourcedShopItemRepository(EventStore eventStore, EventSerializer eventSerializer, ApplicationEventPublisher eventPublisher) {
        this.eventStore = eventStore;
        this.eventSerializer = eventSerializer;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public ShopItem save(ShopItem aggregate) {
        final List<DomainEvent> pendingEvents = aggregate.getUncommittedChanges();
        eventStore.saveEvents(
                aggregate.getUuid(),
                pendingEvents
                        .stream()
                        .map(eventSerializer::serialize)
                        .collect(toList()));
        pendingEvents.forEach(eventPublisher::publishEvent);
        return aggregate.markChangesAsCommitted();
    }

    @Override
    public ShopItem getByUUID(UUID uuid) {
        return ShopItem.from(uuid, getRelatedEvents(uuid));
    }

    @Override
    public ShopItem getByUUIDat(UUID uuid, Instant at) {
        return ShopItem.from(uuid,
                getRelatedEvents(uuid)
                        .stream()
                        .filter(evt -> !evt.when().isAfter(at))
                        .collect(toList()));
    }


    private List<DomainEvent> getRelatedEvents(UUID uuid) {
        return eventStore.getEventsForAggregate(uuid)
                .stream()
                .map(eventSerializer::deserialize)
                .collect(toList());
    }

}

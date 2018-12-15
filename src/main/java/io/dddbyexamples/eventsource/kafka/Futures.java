package io.dddbyexamples.eventsource.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Component;

@Component
public class Futures {

    private Map<UUID, CompletableFuture<String>> futures = new HashMap<>();

    public void complete(UUID uuid) {
        if (futures.containsKey(uuid)) {
            futures.get(uuid).complete("I'm complete!");
        }

    }

    public CompletableFuture<String> add(UUID uuid) {
        CompletableFuture<String> future = new CompletableFuture<>();
        futures.put(uuid, future);
        return future;
    }

}

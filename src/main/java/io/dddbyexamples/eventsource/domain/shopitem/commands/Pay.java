package io.dddbyexamples.eventsource.domain.shopitem.commands;

import lombok.Value;

import java.time.Instant;
import java.util.UUID;

@Value
public class Pay implements Command {

    public static final String TYPE = "Pay";
    private final UUID uuid;
    private final Instant when;

    @Override
    public String type() {
        return TYPE;
    }
}

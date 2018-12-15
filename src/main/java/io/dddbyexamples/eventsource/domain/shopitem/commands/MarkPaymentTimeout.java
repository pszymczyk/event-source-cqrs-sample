package io.dddbyexamples.eventsource.domain.shopitem.commands;

import lombok.Value;

import java.time.Instant;
import java.util.UUID;

@Value
public class MarkPaymentTimeout implements Command {

    public static final String TYPE = "MarkPaymentTimeout";
    private final UUID uuid;
    private final Instant when;

    @Override
    public String type() {
        return TYPE;
    }
}

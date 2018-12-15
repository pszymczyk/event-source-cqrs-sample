package io.dddbyexamples.eventsource.domain.shopitem.commands;

import lombok.Value;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Value
public class Buy implements Command {

    public static final String TYPE = "buy";
    private final UUID uuid;
    private final BigDecimal amount;
    private final Instant when;

    @Override
    public String type() {
        return TYPE;
    }
}

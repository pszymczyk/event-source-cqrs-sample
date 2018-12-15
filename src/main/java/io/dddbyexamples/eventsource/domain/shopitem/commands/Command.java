package io.dddbyexamples.eventsource.domain.shopitem.commands;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = Buy.TYPE, value = Buy.class),
        @JsonSubTypes.Type(name = MarkPaymentTimeout.TYPE, value = MarkPaymentTimeout.class),
        @JsonSubTypes.Type(name = Pay.TYPE, value = Pay.class)
})
public interface Command {

    String type();
}


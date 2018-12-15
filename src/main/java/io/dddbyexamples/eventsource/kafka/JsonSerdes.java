package io.dddbyexamples.eventsource.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

@Component
public class JsonSerdes {

    @Autowired
    private ObjectMapper objectMapper;

    public <T> Serde<T> forA(Class<T> aClass) {
        return serdeFrom(new JsonSerializer<>(objectMapper), new JsonDeserializer<>(aClass, objectMapper));
    }
}

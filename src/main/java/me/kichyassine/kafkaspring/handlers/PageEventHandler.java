package me.kichyassine.kafkaspring.handlers;

import me.kichyassine.kafkaspring.events.PageEvents;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    @Bean
    public Consumer<PageEvents> pageEventConsumer() {
        return (input) ->{
            System.out.println("Received page event: " + input.toString());
        };
    }

    @Bean
    public Supplier<PageEvents> pageEventSupplier() {
        return () -> {
            return new PageEvents(
                    Math.random() > 0.5 ? "Page 1" : "Page 2",
                    Math.random() > 0.5 ? "User 1" : "User 2",
                    new Date(),
                    1222L
            );
        };
    }

    @Bean
    public Function<KStream<String, PageEvents>, KStream<String, Long>> KStreamFunction() {
        return (input) -> input
                .filter((key, value) -> value != null && value.duree() != null && value.duree() >= 100L)
                .map((key, value) -> new KeyValue<>(value.name(), value.duree()));
    }
}

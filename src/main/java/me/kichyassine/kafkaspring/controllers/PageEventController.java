package me.kichyassine.kafkaspring.controllers;

import me.kichyassine.kafkaspring.events.PageEvents;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class PageEventController {


    @Autowired
    private StreamBridge streamBridge;


    @GetMapping("/publish")
    public PageEvents publish(String name , String topic) {
        PageEvents event = new PageEvents(name , Math.random()<0.5?"U1":"U2" , new Date() , 1000L);
        streamBridge.send(topic , event);
        return event;
    }
}

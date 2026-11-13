package me.kichyassine.kafkaspring.events;

import java.util.Date;

public record PageEvents(String name, String user , Date date , Long duree) {

}

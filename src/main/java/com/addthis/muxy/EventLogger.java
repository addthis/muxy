package com.addthis.muxy;

import java.util.Arrays;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventLogger<E extends Enum<E>> implements MuxyEventListener<E> {

    private static final Logger log = LoggerFactory.getLogger(EventLogger.class);

    final String name;
    final Set<E> watchedEvents;

    EventLogger(String name) { // 'verbose'
        this(name, null);
    }

    EventLogger(String name, Set<E> watchedEvents) {
        this.name = name;
        this.watchedEvents = watchedEvents;
    }

    @Override
    public void event(E event, Object target) {
        if ((watchedEvents == null) || watchedEvents.contains(event)) {
            if (target.getClass().isArray()) {
               target = Arrays.toString((Object[]) target);
            }
            log.info("{} event {} message {}", name, event.name(), target);
        }
    }
}

package com.addthis.muxy;

import java.util.Arrays;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventLogger implements MuxyEventListener {

    private static final Logger log = LoggerFactory.getLogger(EventLogger.class);

    final String name;
    final Set<?> watchedEvents;

    EventLogger(String name) { // 'verbose'
        this(name, null);
    }

    EventLogger(String name, Set<?> watchedEvents) {
        this.name = name;
        this.watchedEvents = watchedEvents;
    }

    public void event(Enum<?> event, Object target) {
        if ((watchedEvents == null) || watchedEvents.contains(event)) {
            if (target.getClass().isArray()) {
               target = Arrays.toString((Object[]) target);
            }
            log.info("{} event {} message {}", name, event.name(), target);
        }
    }

    @Override
    public void reportWrite(long bytes) {
        MuxFileDirectoryCache.DEFAULT.reportWrite(bytes);
    }

    @Override
    public void streamEvent(MuxyStreamEvent event, Object target) {
        event(event, target);
    }

    @Override
    public void fileEvent(MuxyFileEvent event, Object target) {
        event(event, target);
    }
}

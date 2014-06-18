/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

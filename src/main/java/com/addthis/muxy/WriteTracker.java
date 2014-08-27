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

public interface WriteTracker {

    /**
     * Should be called to report any change in the number of bytes that waiting to be flushed.
     *
     * Must not be called with a positive value while holding any locks that would prevent flushing
     * pending writes, or optimistic close attempts (ie. waitForWriteClosure(0)).
     */
    void reportWrite(long bytes);

    /**
     * Should be called to report any change in the number of streams.
     *
     * Must not be called with a positive value while holding any locks that would prevent flushing
     * pending writes, or optimistic close attempts (ie. waitForWriteClosure(0)).
     */
    void reportStreams(long streams);
}

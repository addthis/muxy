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

/**
 */
public enum MuxyStreamEvent {
    LOG_READ,
    LOG_COMPACT,
    STREAM_CREATE,
    STREAM_READ,
    STREAM_APPEND,
    STREAM_CLOSE,
    STREAM_CLOSED_ALL,
    STREAM_DELETE,
    BLOCK_FILE_FREED,
    BLOCK_FILE_WRITE,
    BLOCK_FILE_WRITE_OPEN,
    BLOCK_FILE_WRITE_ROLL,
    BLOCK_FILE_WRITE_CLOSE,
    BLOCK_FILE_READ_OPEN,
    BLOCK_FILE_READ_CLOSE,
    CLOSED_ALL_STREAM_WRITERS,
    WRITE_LOCK_ACQUIRED,
    WRITE_LOCK_RELEASED,
}

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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.List;
import java.util.stream.IntStream;

public interface MuxFile {
    String getName();

    IntStream getStreamIds();

    List<MuxStream> getStreams() throws IOException;

    long getLength();

    long getLastModified();

    void writeRecord(OutputStream out) throws IOException;

    InputStream read(long offset, boolean uncompress) throws IOException;

    InputStream read(long offset) throws IOException;

    InputStream read() throws IOException;

    String detail() throws IOException;
}

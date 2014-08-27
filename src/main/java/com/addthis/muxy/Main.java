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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collection;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.Strings;

public class Main {

    private static final DecimalFormat numbers = new DecimalFormat("#,###");
    private static final DateFormat dates = new SimpleDateFormat("MMM dd yyyy, HH:mm:ss");

    private static void usage() {
        System.out.println("file multiplexer usage:");
//        System.out.println("  append [file]          - append stdin to this file");
        System.out.println("  cat [file_list]        - cat file list contents to stdout");
        System.out.println("  defrag [dir]           - defragment all files");
        System.out.println("  info [file]            - detailed file info");
        System.out.println("  block-stat [dir]       - detailed block info");
        System.out.println("  ls [-ld] [dir]         - file list. -l long. -d detail.");
        System.out.println("  mode [file] [new_mode] - set file mode to: ra, a, r");
        System.out.println("  mv [file] [new_name]   - single file rename");
        System.out.println("  rm [file]              - delete file reference");
        System.out.println("  scat [stream_id]       - cat named stream to stdout");
        System.exit(1);
    }

    private static void dumpInputToOutput(InputStream in, OutputStream out) throws IOException {
        if (in == null || out == null) {
            return;
        }
        byte[] buf = new byte[1024];
        int read = 0;
        while ((read = in.read(buf)) > 0) {
            out.write(buf, 0, read);
        }
        in.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && args[0].equals("ls")) {
            Collection<ReadMuxFile> list = null;
            if (args[1].equals("-l")) {
                list = ReadMuxFileDirectoryCache.getResolvedInstance(new File(args.length > 2 ? args[2] : ".")).listFiles();
                int longestName = 9;
                int longestSize = 0;
                for (ReadMuxFile meta : list) {
                    longestName = Math.max(longestName, meta.getName().length());
                    longestSize = Math.max(longestSize, numbers.format(meta.getLength()).length());
                }
                System.out.println(Strings.padright("filename", longestName + 1) + "  last modified         streams  mode  size");
                for (ReadMuxFile meta : list) {
                    String mode = "RA";
                    System.out.println(
                                       Strings.padright(meta.getName(), longestName + 1) +
                                       "  " + dates.format(meta.getLastModified()) +
                                       " " + Strings.padleft(meta.getStreamIDs().size() + "", 7) +
                                       "    " + mode +
                                       "  " + Strings.padleft(numbers.format(meta.getLength()), longestSize)
                                       );
                }
            } else {
                list = ReadMuxFileDirectoryCache.getResolvedInstance(new File(args.length > 1 ? args[1] : ".")).listFiles();
                int count = 0;
                for (ReadMuxFile meta : list) {
                    if (count++ > 0) {
                        System.out.print("\t");
                    }
                    System.out.print(meta.getName());
                }
                System.out.println();
            }
        } else if (args.length > 1 && args[0].equals("cat")) {
            for (int i = 1; i < args.length; i++) {
                File file = new File(args[i]);
                ReadMuxFileDirectory mfm = ReadMuxFileDirectoryCache.getResolvedInstance(file.getParentFile());
                if (file.getName().equals("*")) {
                    for (ReadMuxFile meta : mfm.listFiles()) {
                        dumpInputToOutput(meta.read(0), System.out);
                    }
                } else {
                    dumpInputToOutput(mfm.openFile(file.getName(), false).read(0), System.out);
                }
            }
        } else if (args.length > 1 && args[0].equals("scat")) {
            for (int i = 1; i < args.length; i++) {
                File file = new File(args[i]);
                ReadMuxFileDirectory mfm = ReadMuxFileDirectoryCache.getResolvedInstance(file.getParentFile());
                dumpInputToOutput(mfm.getStreamManager().findStream(Integer.parseInt(file.getName())).read(), System.out);
            }
        } else if (args.length > 1 && args[0].equals("append")) {
            //TODO: fix append
            // File file = new File(args[1]);
            // OutputStream out = ReadMuxFileDirectoryCache.getResolvedInstance(file.getParentFile()).openFile(file.getName(), true).append();
            // dumpInputToOutput(System.in, out);
            // out.close();
        } else if (args.length > 1 && args[0].equals("defrag")) {
            for (int i = 1; i < args.length; i++) {
                File file = new File(args[i]);
                MuxFileDirectoryCache.getWriteableInstance(file.getParentFile()).defrag();
            }
        } else if ((args.length >= 1) && "block-stat".equals(args[0])) {
            File file = new File(".");
            if (args.length >= 2) {
                file = new File(args[1]);
            }
            ReadMuxFileDirectoryCache.getResolvedInstance(file.getParentFile()).getStreamManager().blockStat();
        } else if (args.length > 1 && args[0].equals("info")) {
            File file = new File(args[1]);
            String details = ReadMuxFileDirectoryCache.getResolvedInstance(file.getParentFile())
                                                      .openFile(file.getName(), false)
                                                      .detail();
            System.out.println(details);
        } else if (args.length == 3 && args[0].equals("mv")) {
            File oldName = new File(args[1]);
            File newName = new File(args[2]);
            MuxFile meta = MuxFileDirectoryCache.getWriteableInstance(oldName.getParentFile()).openFile(oldName.getName(), false);
            if (meta == null) {
                System.out.println("rename " + oldName + " to " + newName + " failed");
            }
        } else {
            usage();
        }
        MuxFileDirectoryCache.waitForWriteClosure();
    }
}

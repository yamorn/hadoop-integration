/**
 * Copyright 2012 Kamran Zafar
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sun.hadoop.hdfs.ext.tar;

import java.io.File;

/**
 * Created by yamorn on 14-11-10.
 */

public class TarUtils {
    /**
     * Determines the tar file size of the given folder/file path
     *
     * @param path
     * @return
     */
    public static long calculateTarSize(File path) {
        return tarSize(path) + TarConstants.EOF_BLOCK;
    }

    private static long tarSize(File dir) {
        long size = 0;

        if (dir.isFile()) {
            return entrySize(dir.length());
        } else {
            File[] subFiles = dir.listFiles();

            if (subFiles != null && subFiles.length > 0) {
                for (File file : subFiles) {
                    if (file.isFile()) {
                        size += entrySize(file.length());
                    } else {
                        size += tarSize(file);
                    }
                }
            } else {
                // Empty folder header
                return TarConstants.HEADER_BLOCK;
            }
        }

        return size;
    }

    private static long entrySize(long fileSize) {
        long size = 0;
        size += TarConstants.HEADER_BLOCK; // Header
        size += fileSize; // File size

        long extra = size % TarConstants.DATA_BLOCK;

        if (extra > 0) {
            size += (TarConstants.DATA_BLOCK - extra); // pad
        }

        return size;
    }

    public static String trim(String s, char c) {
        StringBuffer tmp = new StringBuffer(s);
        for (int i = 0; i < tmp.length(); i++) {
            if (tmp.charAt(i) != c) {
                break;
            } else {
                tmp.deleteCharAt(i);
            }
        }

        for (int i = tmp.length() - 1; i >= 0; i--) {
            if (tmp.charAt(i) != c) {
                break;
            } else {
                tmp.deleteCharAt(i);
            }
        }

        return tmp.toString();
    }

    public static boolean isTarEntryHeader(byte[] header) {
        boolean result = false;

        // step 1: validate magic
        String magic = TarHeader.parseName(header, 257, 8).toString();
        result = "ustar  ".equals(magic);
        if (result) return true;

        // step 2: validate checksum
        long sum = 0;
        for (int i = 0; i < header.length; ++i) {
            if (i > 148 && i < 156) {
                sum += (char) ' ';
                continue;
            }
            sum += 255 & header[i];
        }

        long checkSum = Octal.parseOctal(header, 148, 8);
        result = checkSum == sum;
        return result;
    }

    private static long computeCheckSum(byte[] buf) {
        long sum = 0L;
        for (byte aBuf : buf) {
            sum += (long) (255 & aBuf);
        }
        return sum;
    }
}

package sun.hadoop.hdfs.ext.zip;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by yamorn on 2014/12/3.
 */
public class ZipWrapInputStream extends FilterInputStream {
    private static byte[] eocdSign = new byte[]{0x50, 0x4B, 0x05, 0x06};//end of central directory record signature
    private static byte[] socdSign = new byte[]{0x50, 0x4B, 0x01, 0x02}; // start of central directory file header signature
    private ZipInputStream zipInputStream;
    private long markPosition;
    private ZipEntry currentEntry;
    private ZipWrapEntry zipWrapEntry;
    private ZipContent zipContent;

    public ZipWrapInputStream(InputStream in) {
        super(in);
        zipInputStream = new ZipInputStream(in);
        if (!(in instanceof Seekable) || !(in instanceof PositionedReadable)) {
            throw new IllegalArgumentException(
                    "In is not an instance of Seekable or PositionedReadable");
        }
    }

    public synchronized ZipEntry getNextEntry() throws IOException {
        return currentEntry;
    }

    public synchronized ZipContent getZipContent() {
        return zipContent;
    }

    public synchronized ZipWrapEntry getZipWrapEntry() {
        return zipWrapEntry;
    }

    public synchronized boolean hasNextEntry() throws IOException {
        ZipEntry zipEntry = zipInputStream.getNextEntry();
        if (zipEntry != null) {
            currentEntry = zipEntry;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(zipInputStream, outputStream, 4096);
            zipContent = new ZipContent(outputStream.toByteArray());
            outputStream.close();
            zipWrapEntry = new ZipWrapEntry(zipEntry);
        }
        return zipEntry != null;
    }

    /**
     * Get central directory file header offset
     *
     * @param fileLength zip file length
     * @return Relative offset of zip file
     * @throws IOException
     */
    public synchronized long getSOCDStartIndex(long fileLength) throws IOException {
        int index;
        long size = fileLength - 1;
        seek(size);
        int span = 50;
        if (span > fileLength) span = (int) fileLength;
        byte[] buffer = new byte[span];
        int len = -1;
        do {
            for (int i = 0; i <= eocdSign.length; i++) {
                seek(size - span - i);
                _mark(getPos());
                len = in.read(buffer, 0, buffer.length);
                if ((index = KMPMatch.indexOf(buffer, eocdSign)) != -1) {
                    long assumePos = markPosition + index;
                    seek(assumePos + 16);
                    byte[] offset = new byte[4];
                    readFully(offset, 0, offset.length);
                    long socdPos = get32(offset, 0);
                    byte[] compared = new byte[4];
                    seek(socdPos);
                    readFully(compared, 0, compared.length);
                    if (Hex.encodeHexString(socdSign).equals(Hex.encodeHexString(compared).toLowerCase())) {
                        return socdPos;
                    }
                }
                _reset();
            }
            span += span;
        } while (len != -1);
        return -1;
    }

    public synchronized CDHeader getNextCDFH() throws IOException {
        _mark(getPos());
        byte[] sign = new byte[4];
        readFully(sign, 0, sign.length);
        if (!Hex.encodeHexString(socdSign).equals(Hex.encodeHexString(sign))) return null;
        _reset();
        byte[] buffer = new byte[46];
        readFully(buffer, 0, buffer.length);
        int n = get16(buffer, 28);
        int m = get16(buffer, 30);
        int k = get16(buffer, 32);
        long offset = get32(buffer, 42);
        byte[] ext = new byte[n + m + k];
        readFully(ext, 0, ext.length);
        String name = new String(ext, 0, n, "UTF-8");
        CDHeader cdHeader = new CDHeader();
        cdHeader.setFileName(name);
        cdHeader.setLochOffset(offset);
        return cdHeader;
    }

    public ZipInputStream getZipInputStream() {
        return zipInputStream;
    }

    private void readFully(byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            int n = in.read(b, off, len);
            if (n == -1) {
                throw new EOFException();
            }
            off += n;
            len -= n;
        }
    }

    private synchronized void _reset() throws IOException {
        seek(markPosition);
    }

    private synchronized void _mark(long readlimit) {
        markPosition = readlimit;
    }

    public synchronized void seek(long l) throws IOException {
        ((Seekable) in).seek(l);
    }

    public synchronized long getPos() throws IOException {
        return ((Seekable) in).getPos();
    }

    private static int get16(byte b[], int off) {
        return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8);
    }

    private static long get32(byte b[], int off) {
        return (get16(b, off) | ((long) get16(b, off + 2) << 16)) & 0xffffffffL;
    }

    private static long get64(byte b[], int off) {
        return get32(b, off) | (get32(b, off + 4) << 32);
    }
}

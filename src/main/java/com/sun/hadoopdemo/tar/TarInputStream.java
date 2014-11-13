
package com.sun.hadoopdemo.tar;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Kamran Zafar,Louis
 */
public class TarInputStream extends FilterInputStream {
	private static final int SKIP_BUFFER_SIZE = 2048;
	private TarEntry currentEntry;
	private long currentFileSize;
	private long readPastMark;

	private TarEntry markCurrentEntry;
	private long markCurrentFileSize;

	public TarInputStream(InputStream in) {
		super(in);
		if (!(in instanceof Seekable) || !(in instanceof PositionedReadable)) {
			throw new IllegalArgumentException(
					"In is not an instance of Seekable or PositionedReadable");
		}
		currentFileSize = 0;
		readPastMark = 0;
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public synchronized void mark(int readlimit) {
	}

	@Override
	public synchronized void reset() throws IOException {
	}

	/**
	 * private method
	 *
	 * @param readlimit Must be in multiples of 512
	 */
	private synchronized void _mark(int readlimit) {
		readPastMark = readlimit;
		markCurrentEntry = currentEntry;
		markCurrentFileSize = currentFileSize;
	}

	/**
	 * private method
	 *
	 * @throws IOException
	 */
	private synchronized void _reset() throws IOException {
		seek(readPastMark);
		currentEntry = markCurrentEntry;
		currentFileSize = markCurrentFileSize;
	}

	/**
	 * Read a byte
	 *
	 * @see java.io.FilterInputStream#read()
	 */
	@Override
	public synchronized int read() throws IOException {
		byte[] buf = new byte[1];
		int res = this.read(buf, 0, 1);
		if (res != -1) {
			return 0xFF & buf[0];
		}
		return res;
	}

	public synchronized TarEntry getCurrentEntry() {
		return currentEntry;
	}

	/**
	 * Checks if the bytes being read exceed the entry size and adjusts the byte
	 * array length. Updates the byte counters
	 *
	 * @see java.io.FilterInputStream#read(byte[], int, int)
	 */
	@Override
	public synchronized int read(byte[] b, int off, int len) throws IOException {
		if (currentEntry != null) {
			if (currentFileSize == currentEntry.getSize()) {
				return -1;
			} else if ((currentEntry.getSize() - currentFileSize) < len) {
				len = (int) (currentEntry.getSize() - currentFileSize);
			}
		}

		int br = super.read(b, off, len);

		if (br != -1) {
			if (currentEntry != null) {
				currentFileSize += br;
			}
		}

		return br;
	}

	/**
	 * Returns the next entry in the tar file
	 *
	 * @return TarEntry
	 * @throws java.io.IOException
	 */
	private synchronized TarEntry getNextEntry() throws IOException {
		closeCurrentEntry();
		byte[] header = new byte[TarConstants.HEADER_BLOCK];
		byte[] theader = new byte[TarConstants.HEADER_BLOCK];
		int tr = 0;

		// Read full header
		while (tr < TarConstants.HEADER_BLOCK) {
			int res = read(theader, 0, TarConstants.HEADER_BLOCK - tr);

			if (res < 0) {
				break;
			}

			System.arraycopy(theader, 0, header, tr, res);
			tr += res;
		}
		// Check if record is null
		boolean eof = true;
		for (byte b : header) {
			if (b != 0) {
				eof = false;
				break;
			}
		}

		if (!eof) {
			_mark((int) getPos());

			long realSize = Octal.parseOctal(header, 124, 12);
			byte[] content = new byte[(int) realSize];
			//read content
			int d = super.read(content, 0, (int) realSize);
			if (d == -1) {
				// I suspect file corruption
				throw new IOException("Possible tar file corruption");
			}
			_reset();
			currentEntry = new TarEntry(header, content);
		}
		return currentEntry;
	}

	/**
	 * Closes the current tar entry
	 *
	 * @throws java.io.IOException
	 */
	protected synchronized void closeCurrentEntry() throws IOException {
		if (currentEntry != null) {
			if (currentEntry.getSize() > currentFileSize) {
				// Not fully read, skip rest of the bytes
				long bs = 0;
				while (bs < currentEntry.getSize() - currentFileSize) {
					long res = skip(currentEntry.getSize() - currentFileSize - bs);

					if (res == 0 && currentEntry.getSize() - currentFileSize > 0) {
						// I suspect file corruption
						throw new IOException("Possible tar file corruption");
					}

					bs += res;
				}
			}

			currentEntry = null;
			currentFileSize = 0L;
			skipPad();
		}
	}

	/**
	 * Skips the pad at the end of each tar entry file content
	 *
	 * @throws java.io.IOException
	 */
	protected synchronized void skipPad() throws IOException {
		if (getPos() > 0) {
			int extra = (int) (getPos() % TarConstants.DATA_BLOCK);

			if (extra > 0) {
				long bs = 0;
				while (bs < TarConstants.DATA_BLOCK - extra) {
					long res = skip(TarConstants.DATA_BLOCK - extra - bs);
					bs += res;
				}
			}
		}
	}

	/**
	 * Skips 'n' bytes on the InputStream
	 * Overrides default implementation of skip
	 */
	@Override
	public synchronized long skip(long n) throws IOException {
		if (n <= 0) {
			return 0;
		}

		long left = n;
		byte[] sBuff = new byte[SKIP_BUFFER_SIZE];

		while (left > 0) {
			int res = read(sBuff, 0, (int) (left < SKIP_BUFFER_SIZE ? left : SKIP_BUFFER_SIZE));
			if (res < 0) {
				break;
			}
			left -= res;
		}

		return n - left;
	}

	public synchronized boolean hasNextTarEntry() throws IOException {
//		_mark((int)bytesRead);
//		closeCurrentEntry();
//		byte[] header = new byte[TarConstants.HEADER_BLOCK];
//		byte[] theader = new byte[TarConstants.HEADER_BLOCK];
//		int tr = 0;
//
//		// Read full header
//		while (tr < TarConstants.HEADER_BLOCK) {
//			int res = super.read(theader, 0, TarConstants.HEADER_BLOCK - tr);
//			if (res < 0) {
//				break;
//			}
//			System.arraycopy(theader, 0, header, tr, res);
//			tr += res;
//		}
//
//		// Check if record is null
//		boolean eof = true;
//		for (byte b : header) {
//			if (b != 0) {
//				eof = false;
//				break;
//			}
//		}
//		_reset();
//		return !eof;
		return getNextEntry() != null;
	}

	public synchronized long indexTarEntryHeader(long offset, long end) throws IOException {
		_mark((int) offset);
		long index = offset;
		byte[] header = new byte[TarConstants.HEADER_BLOCK];
		byte[] theader = new byte[TarConstants.HEADER_BLOCK];
		do {
			int tr = 0;
			// Read full header
			while (tr < TarConstants.HEADER_BLOCK) {
				int res = read(theader, 0, TarConstants.HEADER_BLOCK - tr);

				if (res < 0) {
					break;
				}

				System.arraycopy(theader, 0, header, tr, res);
				tr += res;
			}
		} while (!TarUtils.isTarEntryHeader(header) && (index = index + TarConstants.HEADER_BLOCK) <= end);
		if (index > end) {
			throw new RuntimeException("Out of block size.");
		}
		_reset();
		return index;
	}

	/**
	 * @param l Must be in multiples of 512
	 * @throws IOException
	 */
	public synchronized void seek(long l) throws IOException {
		((Seekable) in).seek(l);
	}

	/**
	 * Returns the current offset (in bytes) from the beginning of the stream.
	 * This can be used to find out at which point in a tar file an entry's content begins, for instance.
	 */

	public synchronized long getPos() throws IOException {
		return ((Seekable) in).getPos();
	}
}

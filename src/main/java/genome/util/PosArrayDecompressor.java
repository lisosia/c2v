package genome.util;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import genome.GenomeDataStoreUtil.CompressedBuffer;

public class PosArrayDecompressor {
	private DataInputStream decmpBuf;
	private int lastReadPos;

	public PosArrayDecompressor(byte[] buf) throws IOException {
		decmpBuf = new DataInputStream(CompressedBuffer.compressedByteArray2InputStream(buf));
		lastReadPos = decmpBuf.readInt();
	}

	public void close() throws IOException {
		if (decmpBuf != null) {
			decmpBuf.close();
		}
	}

	/**
	 * 
	 * @return nextPostion(int) 全て読みきった後は -1 を返す
	 * @throws IOException
	 */
	public int readNextPos() throws IOException {
		int read;
		try {
			read = decmpBuf.readInt();
		} catch (EOFException e) {
			return -1;
		}
		assert read > 0;
		lastReadPos += read;
		return lastReadPos;
	}
}
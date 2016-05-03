package genome.util;

import java.io.IOException;

import genome.GenomeDataStoreUtil.CompressedBuffer;

/**
 * Postion(0以上の,単調増加する整数列を圧縮してByteArrayを出力する)
 */
public class PosArrayCompressor {
	private int last_written_pos;
	private CompressedBuffer cmpBuf;

	public PosArrayCompressor(int first_referenced_position) throws IOException {
		this.last_written_pos = first_referenced_position;
		cmpBuf = new CompressedBuffer();
		cmpBuf.writeInt(first_referenced_position);
	}

	public void writePos(int pos) throws IOException {
		if (pos < last_written_pos) {
			throw new IllegalArgumentException(
					"postion:" + pos + " is smaller than last_written_postion:" + last_written_pos);
		}
		cmpBuf.writeInt(pos - last_written_pos);
		last_written_pos = pos;
	}

	public byte[] toByteArray() throws IOException {
		return cmpBuf.toByteArray();
	}

	public void close() throws IOException {
		cmpBuf.close();
	}

}
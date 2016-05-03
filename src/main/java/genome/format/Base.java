package genome.format;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import genome.GenomeDataStoreUtil.CompressedBuffer;

/**
 * 塩基配列を表現するEnum. 内部クラスでは, Base列を圧縮/解凍する
 */
public enum Base {
	Base_A(0), Base_C(1), Base_G(2), Base_T(3);
	private final int id;

	private Base(int n) {
		this.id = n;
	}

	public int getNum() {
		return this.id;
	}

	static public int parseStringAndGetID(String base) {
		switch (base) {
		case "A":
			return Base_A.getNum();
		case "C":
			return Base_C.getNum();
		case "G":
			return Base_G.getNum();
		case "T":
			return Base_T.getNum();
		default:
			throw new IllegalArgumentException();
		}
	}

	public static int returnDiff(String reference, String base) {
		if (reference.equals(base)) {
			return 0;
		}
		return (byte) ((4 + parseStringAndGetID(base) - parseStringAndGetID(reference)) % 4);
	}

	public static int returnBase(String reference, int baseDiff) {
		int id = (parseStringAndGetID(reference) + baseDiff) % 4;
		return id;
	}

	/**
	 * 塩基(referenceとSampleから取得できたACGT)の列を圧縮してByteArrayを出力する)
	 */
	public static class BaseArrayCompressor {
		private CompressedBuffer cmpBuf;

		public BaseArrayCompressor() throws IOException {
			cmpBuf = new CompressedBuffer();
		}

		/**
		 * 
		 * @param diffs
		 *            2要素のint配列,(byte)( diffs[0] << 2 | diffs[1] ) を書き込む
		 * @throws IOException
		 */
		public void writeDiffs(int diff1, int diff2) throws IOException {
			cmpBuf.write8bit((byte) (diff1 << 4 | diff2));
		}

		/*
		 * public void writeBase( String ref, String base ) throws IOException {
		 * cmpBuf.write8bit( Base.returnDiff(ref, base) ); }
		 */
		public void writeBase_WhenAlt(String ref, String base1, String base2) throws IOException {
			if (base1 == base2 && ref.equals(base1)) { // optimization
				cmpBuf.write8bit(0);
				return;
			}
			byte b = (byte) ((Base.returnDiff(ref, base1) << 4) | (Base.returnDiff(ref, base2)));
			cmpBuf.write8bit(b);
		}

		public byte[] toByteArray() throws IOException {
			return cmpBuf.toByteArray();
		}

		public void close() throws IOException {
			cmpBuf.close();
		}

	}

	public static class BaseArrayDeCompressor { // TODO VCF出力の時に使う
		private DataInputStream decmpBuf;

		public BaseArrayDeCompressor(byte[] buf) throws IOException {
			decmpBuf = new DataInputStream(CompressedBuffer.compressedByteArray2InputStream(buf));
		}

		public void close() throws IOException {
			if (decmpBuf != null) {
				decmpBuf.close();
			}
		}

		/**
		 * 
		 * @param buf
		 *            length >= 2 となるint配列
		 * @return もう読めない時は -1 を返す
		 * @throws IOException
		 */
		public int readNextBaseDiff(int[] buf) throws IOException {
			byte b;
			try {
				b = decmpBuf.readByte();
			} catch (EOFException e) {
				return -1;
			}
			buf[0] = ((b & 0b11110000) >> 4);
			buf[1] = (b & 0b00001111);
			return 0;
		}
	}

}
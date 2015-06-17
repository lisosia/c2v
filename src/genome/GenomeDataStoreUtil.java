package genome;

import genome.chr.Chr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

final public class GenomeDataStoreUtil {
	private static final int STORE_TIMEOUT_MS = 100 * 60 * 1000;
	public static final class PersonalID { // GenomeData特定用ID, 不変クラス
		private final String runID;
		private final String sampleID;

		public PersonalID(final String runID, final String sampleID) {
			this.runID = runID;
			this.sampleID = sampleID;
		}

		public String getRunID() {
			return runID;
		}

		public String getSampleName() {
			return sampleID;
		}
	}

	public static final class PersonalGenomeDataCompressor {
		private final Connection con;
		private final Chr chr;
		private int pos_index;
		private PositionArrayCompressor posBuf;
		private BaseArrayCompressor baseBuf;
		private final PersonalID pid;

		public PersonalGenomeDataCompressor(PersonalID id, Connection con,
				Chr chr, int pos_index) throws IOException {
			this.pid = id;
			this.con = con;
			this.chr = chr;
			this.pos_index = pos_index;
			posBuf = new PositionArrayCompressor(pos_index);
			baseBuf = new BaseArrayCompressor();
		}

		public void writeData(int pos, int diff1, int diff2) throws IOException {
			posBuf.writePos(pos);
			baseBuf.writeDiffs(diff1, diff2);
		}

		// TODO sql insert( , , ,) values( , , , )に変える
		public void StoreDB(String tableName) throws SQLException, IOException {
			String sql = "INSERT INTO " + tableName + " VALUES(?,?,?,?,?)";
			PreparedStatement ps = this.con.prepareStatement(sql);
			ps.setQueryTimeout(STORE_TIMEOUT_MS);
			ps.setInt(1, chr.getNumForDB() );
			ps.setInt(2, this.pos_index);
			ps.setString(3, pid.getSampleName());
			ps.setBytes(4, this.posBuf.toByteArray());
			ps.setBytes(5, this.baseBuf.toByteArray());

			// TODO executeだけして　updateは後で良い?
			ps.executeUpdate();
		}

		/**
		 * pos_index のみを変えて, 内部Bufferを再利用
		 * 
		 * @param pos_index
		 */
		public void resetBuffer(int pos_index) throws IOException {
			this.pos_index = pos_index;
			// TODO 作りなおすのもったいない??
			posBuf.close();
			baseBuf.close();
			posBuf = new PositionArrayCompressor(pos_index);
			baseBuf = new BaseArrayCompressor();
		}

		public void close() throws IOException {
			posBuf.close();
			baseBuf.close();
		}
	}

	public static class PersonalGenomeDataDecompressor {
		private PositionArrayDeCompressor pos;
		private BaseArrayDeCompressor base;

		PersonalGenomeDataDecompressor(byte[] compressed_pos_array,
				byte[] compressed_base_array) throws IOException {
			pos = new PositionArrayDeCompressor(compressed_pos_array);
			base = new BaseArrayDeCompressor(compressed_base_array);
		}

		/**
		 * 
		 * @param buf
		 *            length >= 3 となる int 配列, [base1,base2, position] で埋める
		 * @return 読みきった時は -1
		 * @throws IOException
		 */
		int readNext(int[] buf) throws IOException {
			base.readNextBaseDiff(buf);
			if ((buf[2] = pos.readNextPos()) == -1) {
				pos.close();
				base.close();
				return -1;
			}
			return 0;
		}
	}

	/**
	 * Postion(0以上の,単調増加する整数列を圧縮してByteArrayを出力する)
	 */
	static class PositionArrayCompressor {
		private int last_written_pos;
		private CompressedBuffer cmpBuf;

		public PositionArrayCompressor(int first_referenced_position)
				throws IOException {
			this.last_written_pos = first_referenced_position;
			cmpBuf = new CompressedBuffer();
			cmpBuf.writeInt(first_referenced_position);
		}

		public void writePos(int pos) throws IOException {
			if (pos < last_written_pos) {
				throw new IllegalArgumentException("postion:" + pos
						+ " is smaller than last_written_postion:"
						+ last_written_pos);
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

	static class PositionArrayDeCompressor {
		private DataInputStream decmpBuf;
		private int lastReadPos;

		public PositionArrayDeCompressor(byte[] buf) throws IOException {
			decmpBuf = new DataInputStream(
					CompressedBuffer.compressedByteArray2InputStream(buf));
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

	/**
	 * 塩基(referenceとSampleから取得できたACGT)の列を圧縮してByteArrayを出力する)
	 */
	static class BaseArrayCompressor {
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
		public void writeBase_WhenAlt(String ref, String base1, String base2)
				throws IOException {
			if (base1 == base2 && ref.equals(base1)) { // optimization
				cmpBuf.write8bit(0);
				return;
			}
			byte b = (byte) ((Base.returnDiff(ref, base1) << 4) | (Base
					.returnDiff(ref, base2)));
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
			decmpBuf = new DataInputStream(
					CompressedBuffer.compressedByteArray2InputStream(buf));
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
			buf[1] = ( b & 0b00001111);
			return 0;
		}
	}

	static public enum Base {
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
	}

	static final class CompressedBuffer { // UTGBのコードが参考
		private ByteArrayOutputStream buf;
		private CompressorOutputStream compressor;
		private DataOutputStream dos;

		public CompressedBuffer() throws IOException {
			buf = new ByteArrayOutputStream();
			reset();
		}

		public final void write(byte[] data) throws IOException {
			compressor.write(data);
		}

		public void writeInt(int n) throws IOException {
			dos.writeInt(n);
		}

		/**
		 * int n の下位8bitを書き込む
		 * 
		 * @param n
		 * @throws IOException
		 */
		public final void write8bit(int n) throws IOException {
			dos.write(n);
		}

		public final byte[] toByteArray() throws IOException {
			compressor.close();
			final byte[] ret = buf.toByteArray();
			return ret;
		}

		public final void close() throws IOException {
			dos.close();
			compressor.close();
			buf.close();
			dos = null;
			compressor = null;
			buf = null;
		}

		public final void reset() throws IOException {
			buf.reset();
			compressor = new BZip2CompressorOutputStream(buf, 1);
			dos = new DataOutputStream(compressor);
		}

		public static InputStream compressedByteArray2InputStream(byte[] buf)
				throws IOException {
			return new BZip2CompressorInputStream(new ByteArrayInputStream(buf));
		}
	}

	public static void main(String args[]) throws Exception { // For Test
		PositionArrayCompressor pb = new PositionArrayCompressor(0);
		pb.writePos(12);
		pb.writePos(13);
		pb.writePos(14);
		byte[] cmp = pb.toByteArray();
		PositionArrayDeCompressor res = new PositionArrayDeCompressor(cmp);
		int p;
		while ((p = res.readNextPos()) != -1) {
			System.out.println(p);
		}
	}
}

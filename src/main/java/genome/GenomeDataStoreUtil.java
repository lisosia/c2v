package genome;

import genome.chr.Chr;
import genome.format.Base;
import genome.util.PosArrayCompressor;
import genome.util.PosArrayDecompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
		private PosArrayCompressor posBuf;
		private Base.BaseArrayCompressor baseBuf;
		private final PersonalID pid;

		public PersonalGenomeDataCompressor(PersonalID id, Connection con, Chr chr, int pos_index) throws IOException {
			this.pid = id;
			this.con = con;
			this.chr = chr;
			this.pos_index = pos_index;
			posBuf = new PosArrayCompressor(pos_index);
			baseBuf = new Base.BaseArrayCompressor();
		}

		public void writeData(int pos, int diff1, int diff2) throws IOException {
			posBuf.writePos(pos);
			baseBuf.writeDiffs(diff1, diff2);
		}

		// TODO sql insert( , , ,) values( , , , )に変える
		public void StoreDB(String tableName) throws SQLException, IOException {
			String sql = "INSERT INTO " + tableName + " VALUES(?,?,?,?)";
			PreparedStatement ps = this.con.prepareStatement(sql);
			ps.setQueryTimeout(STORE_TIMEOUT_MS);
			ps.setInt(1, chr.getNumForDB());
			ps.setInt(1, this.pos_index);
			ps.setString(2, pid.getSampleName());
			ps.setBytes(3, this.posBuf.toByteArray());
			ps.setBytes(4, this.baseBuf.toByteArray());

			// TODO executeだけして updateは後で良い?
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
			posBuf = new PosArrayCompressor(pos_index);
			baseBuf = new Base.BaseArrayCompressor();
		}

		public void close() throws IOException {
			posBuf.close();
			baseBuf.close();
		}
	}

	public static class PersonalGenomeDataDecompressor {
		private PosArrayDecompressor pos;
		private Base.BaseArrayDeCompressor base;

		PersonalGenomeDataDecompressor(byte[] compressed_pos_array, byte[] compressed_base_array) throws IOException {
			pos = new PosArrayDecompressor(compressed_pos_array);
			base = new Base.BaseArrayDeCompressor(compressed_base_array);
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

	public static final class CompressedBuffer { // UTGBのコードが参考
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

		public static InputStream compressedByteArray2InputStream(byte[] buf) throws IOException {
			return new BZip2CompressorInputStream(new ByteArrayInputStream(buf));
		}
	}

	public static void main(String args[]) throws Exception { // For Test
		PosArrayCompressor pb = new PosArrayCompressor(0);
		pb.writePos(12);
		pb.writePos(13);
		pb.writePos(14);
		byte[] cmp = pb.toByteArray();
		PosArrayDecompressor res = new PosArrayDecompressor(cmp);
		int p;
		while ((p = res.readNextPos()) != -1) {
			System.out.println(p);
		}
	}
}

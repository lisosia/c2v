package genome;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import genome.GenomeDataStoreUtil.PersonalGenomeDataDecompressor;
import genome.chr.Chr;
import genome.chr.Sex;
import genome.format.ReferenceReader;

public class Merge {

	final AppConfig c;
	final ManageDB MDB;

	public Merge(String configFilePath, String checkSexFilePath) throws FileNotFoundException, IOException  {
		this.c = new AppConfig(configFilePath);
		this.MDB = new ManageDB(configFilePath, checkSexFilePath);
	}

	public void printDiffByChr(Chr chr, Map<String, ArrayList<String>> id, PrintStream out, final boolean printNotAlts)
			throws ClassNotFoundException, SQLException, IOException {

		for (int pos : MDB.getExistingPosIndex(chr, id.keySet())) {
			int[] merged = getMergedData(chr, id, pos);
			new PrintData(chr, c.referenceDBPath.getAbsolutePath(), printNotAlts).printMergedData(merged, pos, out);
		}
	}

	/**
	 * @param id
	 *            Map<runID, ArrayList of sampleIDs> mergeするData特定用
	 */
	int[] getMergedData(Chr chr, Map<String, ArrayList<String>> id, int pos_index)
			throws IOException, SQLException, ClassNotFoundException {
		int[] ret = new int[MergeArrayFormat.SIZE_PER_BASE * (c.DATA_SPLIT_UNIT + 1)];
		for (String runID : id.keySet()) {
			int[] mergedByRunID = getMergedDataByRunID(chr, runID, id.get(runID), pos_index);
			assert (ret.length == mergedByRunID.length);
			for (int i = 0; i < mergedByRunID.length; i++) {
				ret[i] += mergedByRunID[i];
			}
			mergedByRunID = null; // GCを期待 <= 嘘っぽい
		}
		return ret;
	}

	int[] getMergedDataByRunID(Chr chr, String runID, List<String> sampleIDs, int pos_index)
			throws SQLException, ClassNotFoundException, IOException {
		if (sampleIDs.size() == 0) {
			throw new IllegalArgumentException("sampleIDs[of " + runID + "] 's size == 0");
		}
		if (!MDB.dbExists(runID, chr)) {
			throw new IllegalArgumentException("DB does not exist. runID,chr:" + runID + "," + chr);
		}
		StringBuilder sb = new StringBuilder(
				"select * from " + c.TABLE_NAME + " where pos_index = ? and sample_id in (");

		boolean isFirst = true;
		Connection con = MDB.getConnection(runID, chr);
		// con.setReadOnly(true);

		for (String id : sampleIDs) {
			if (!MDB.checkDataExistance(con, runID, id, chr)) {
				throw new Error("[runID,sampleID,chr]:[" + runID + "," + id + "," + chr
						+ "] does not exist! terminate process.");
			}
			if (isFirst) {
				sb.append("'").append(id).append("'");
			} else {
				sb.append(",").append("'").append(id).append("'");
			}
			isFirst = false;
		}
		sb.append(")");
		final String sql = sb.toString();
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setQueryTimeout(c.SQLITE_TIMEOUT_SEC);
		// ps.setInt(1, chr.getNumForDB() ); REMOVED
		ps.setInt(1, pos_index);
		ResultSet rs = ps.executeQuery();

		// for validation
		HashMap<String, Boolean> gotData = new HashMap<String, Boolean>();
		for (String id : sampleIDs) {
			gotData.put(id, false);
		}

		int[] ret = new int[MergeArrayFormat.SIZE_PER_BASE * (c.DATA_SPLIT_UNIT + 1)]; // AA,AC,...,TT,
																						// _A,_B,_C,_T,
																						// isALT
		int gotIdNum = 0;
		while (rs.next()) {
			gotIdNum += 1;
			final String gotID = rs.getString("sample_id");
			assert gotData.containsKey(gotID);
			gotData.put(gotID, true);

			System.err.println("merging   sample_id:" + gotID + " pos_index:" + String.valueOf(pos_index));
			PersonalGenomeDataDecompressor d = new PersonalGenomeDataDecompressor(rs.getBytes("pos_array"),
					rs.getBytes("base_array"));
			int[] data = new int[3];

			while (d.readNext(data) != -1) {
				final int base1 = data[0];
				final int base2 = data[1];
				if (base2 == 0b1111) { // base2は0b11にならない
					System.err.println(" " + base1 + " " + base2 + " " + data[2]);
					throw new IllegalArgumentException("base is 0b1111. internal error");
				}
				final int posRead = data[2];
				try {
					final int BASE_DX = MergeArrayFormat.SIZE_PER_BASE * (posRead - pos_index);
					if ((base1 != 0 && base1 != 0b1111) || base2 != 0) {
						ret[BASE_DX + MergeArrayFormat.IS_ALT_DX] = 1;// flag
																		// for
																		// !=ref
					}
					if (base1 != 0b1111) {
						ret[BASE_DX + MergeArrayFormat.getSubIndex(base1, base2)] += 1;
						ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] += 2;
					} else {
						ret[BASE_DX + MergeArrayFormat.getSubIndex(base1, base2)] += 1; // same
						ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] += 1;
					}

				} catch (ArrayIndexOutOfBoundsException e) {
					System.err.println(
							"pos_index,posread,base1,base2:" + pos_index + "," + posRead + "," + base1 + "," + base2);
					throw new RuntimeException("array index outofBounds", e);
				}
			}

		}

		// Yのときは数が合わなくても、スルーしてしまう
		// !chr.getStr().equals("Y") のとき、大量にMegが出力されるはず、、、どうしようか。
		if (gotData.containsValue(false)) {
			final String preMsg = "missing DB Data ::\n" + "chr:" + chr + ",runID:" + runID + ",position(index):"
					+ pos_index + "\n" + "missing sample_id(s):";
			if (!chr.getStr().equals("Y")) {
				String Msg = "";
				for (Map.Entry<String, Boolean> e : gotData.entrySet()) {
					if (e.getValue() == false) {
						Msg += (e.getKey() + ",");
					}
				}
				System.err.println(preMsg + Msg);
				// throw new IllegalArgumentException("Warning:
				// DBファイル内に,要求されたすべてのサンプルIDが含まれていません");

			} else { // when chrY
				String Msg = "";
				for (Map.Entry<String, Boolean> e : gotData.entrySet()) {
					if (MDB.checkSex.getSex(e.getKey()) == Sex.Female) {
						continue;
					} // Femaleのときは関係なし
					if (e.getValue() == false) {
						Msg += (e.getKey() + ",");
					}
				}
				if (!Msg.equals("")) {
					System.err.println(preMsg + Msg);
				}

			}

		}

		con.close();
		return ret;
	}

}

/**
 * 名前の通り merge で集計するときに使用する baes2ではなくbase1 が、 0b1111 が許される。
 *
 */
class MergeArrayFormat {
	private MergeArrayFormat() {
	}

	static final int SIZE_PER_BASE = 22;
	static final int NO_DATA_BASENUM = 0b1111;
	static final int IS_ALT_DX = 20;
	static final int TOTAL_AN_DX = 21;

	static int getSubIndex(int base1, int base2) {
		if (base1 == NO_DATA_BASENUM) {
			return 4 * 4 + base2;
		} else {
			return 4 * base1 + base2;
		}
	}
}

class PrintData {
	private Chr chr;
	ReferenceReader rr;
	final boolean printNotAlts;

	PrintData(Chr chr, String refDBPath, boolean printNotAlts) throws IOException, SQLException {
		this.chr = chr;
		this.printNotAlts = printNotAlts;
		// TODO WHEN CHR = X/Y
		rr = new ReferenceReader(chr, refDBPath);
	}

	// ここの文脈での base は baseDiff
	void printMergedData(int[] merged, int pos_index, PrintStream out) throws IOException, SQLException {

		if (merged.length % MergeArrayFormat.SIZE_PER_BASE != 0) {
			throw new IllegalArgumentException("arg<merged> 's format is incorrect");
		}

		BaseCounter baseCounter = new BaseCounter();
		/*
		 * forLoopは i: 1 =< i < (DATA_SPLIT_UNIT+1) _注:retで検索 merge~関数 のときは
		 * const*(DATA_SPLIT_UNIT+1) の長さの配列を用意し, index=0は使っていない 少しややこしいので、注意
		 */
		for (int i = 1; i < merged.length / MergeArrayFormat.SIZE_PER_BASE; ++i) {
			int base_dx = MergeArrayFormat.SIZE_PER_BASE * i;
			int absolutePos;
			final String[] ACGT = { "A", "C", "G", "T" };

			if (printNotAlts && merged[base_dx + MergeArrayFormat.TOTAL_AN_DX] == 0) {
				continue;
			}
			if (!printNotAlts && merged[base_dx + MergeArrayFormat.IS_ALT_DX] == 0) {
				continue;
			}

			// printかつalt==0 -> listを省略できる
			if (printNotAlts && merged[base_dx + MergeArrayFormat.IS_ALT_DX] == 0) {

				baseCounter.reset();
				absolutePos = pos_index + i;
				int ref_num = rr.readByNumber(absolutePos);
				assert ref_num != -1;

				///////////////////
				// int list[] = { 0 , 4*4+ref_num };
				///////////////////
				int b_num1 = merged[base_dx + 0];
				if (b_num1 != 0) {
					baseCounter.set(0, 0, ref_num, b_num1);
				}
				int b_num2 = merged[base_dx + 4 * 4];
				if (b_num2 != 0) {
					baseCounter.set(4, 0, ref_num, b_num2);
				}
				//////////

				final int alt_total = merged[base_dx + MergeArrayFormat.TOTAL_AN_DX];
				String INFO = "AN=" + (alt_total) + ";GC=" + baseCounter.getGenomeCntStr();

				out.printf("chr%s\t%d\t%s\t%s\t.\t.\t%s\n", chr.getStr(), absolutePos, ACGT[ref_num],
						baseCounter.getAltsStr(), INFO);

				// altなら出力
			} else if (merged[base_dx + MergeArrayFormat.IS_ALT_DX] != 0) {

				baseCounter.reset();
				absolutePos = pos_index + i;
				int ref_num = rr.readByNumber(absolutePos);
				assert ref_num != -1;
				int size1 = 5;
				int size2 = 4; // size1*size2 < SIZE_PER_BASE
				for (int l = 0; l < size1 * size2; ++l) {
					// comment-out to print "AA" when ref=="A"
					// if(l==0){continue;}
					int b_num = merged[base_dx + l];
					int base1 = l / 4;
					int base2 = l % 4;
					if (b_num != 0) {
						baseCounter.set(base1, base2, ref_num, b_num);
					}
				}
				final int alt_total = merged[base_dx + MergeArrayFormat.TOTAL_AN_DX];
				String INFO = "AN=" + (alt_total) + ";GC=" + baseCounter.getGenomeCntStr();

				out.printf("chr%s\t%d\t%s\t%s\t.\t.\t%s\n", chr.getStr(), absolutePos, ACGT[ref_num],
						baseCounter.getAltsStr(), INFO);

			}
		}
	}

	public static class BaseCounter {
		final private int[] buffer = new int[14]; // aa,ac,ag.at.cc,cg,ct,gg,gt,tt,xA,xC,xG,xT
		String altsStr = null;
		String genomeCntStr = null;

		BaseCounter() {
		}

		void reset() {
			for (int i = 0; i < buffer.length; ++i) {
				buffer[i] = 0;
			}
			this.altsStr = null;
			this.genomeCntStr = null;
		}

		/**
		 * base: acgt(diff to ref)<->[0123], "x" -- 4or4over, "x" should appear
		 * only in base1
		 * 
		 * @param base1
		 * @param base2
		 */
		public void set(int base1, int base2, int ref_num, final int allele_num) {

			// a little tricky
			base2 = (base2 + ref_num) % 4;
			if (base1 >= 4) {
				buffer[10 + base2] = allele_num;
				return;
			}
			base1 = (base1 + ref_num) % 4;

			if (base1 > base2) { // change to (base1 <= base2)
				int tmp = base2;
				base2 = base1;
				base1 = tmp;
			}
			int index = base2;
			switch (base1) {
			case 0:
				break;
			case 1:
				index += (4 - 1);
				break;
			case 2:
				index += (4 + 3 - 2);
				break;
			case 3:
				index += (4 + 3 + 2 - 3);
				break;
			default:
				throw new IllegalArgumentException("InternalError check code");
			}
			buffer[index] = allele_num;
		}

		private void makeStrs() {
			final StringBuilder sb1 = new StringBuilder();
			final StringBuilder sb2 = new StringBuilder();
			for (int i = 0; i < buffer.length; ++i) {
				final int AN = buffer[i];
				if (AN != 0) {
					if (sb1.length() != 0) {
						sb1.append(",");
						sb2.append(",");
					}
					sb1.append(this.getStr(i));
					sb2.append(Integer.toString(AN));
				}
			}
			if (sb1.length() == 0) {
				throw new IllegalArgumentException("internal error. no alts found");
			}
			this.altsStr = sb1.toString();
			this.genomeCntStr = sb2.toString();
		}

		public String getAltsStr() {
			if (this.altsStr == null) {
				makeStrs();
			}
			return this.altsStr;
		}

		public String getGenomeCntStr() {
			if (this.genomeCntStr == null) {
				makeStrs();
			}
			return this.genomeCntStr;
		}

		private String getStr(final int index) {
			final String[] ACGT = { "xA", "xC", "xG", "xT" };
			if (index >= 10) {
				return ACGT[index - 10];
			}
			switch (index) {
			case 0:
				return "AA";
			case 1:
				return "AC";
			case 2:
				return "AG";
			case 3:
				return "AT";
			case 4:
				return "CC";
			case 5:
				return "CG";
			case 6:
				return "CT";
			case 7:
				return "GG";
			case 8:
				return "GT";
			case 9:
				return "TT";
			default:
				throw new IllegalArgumentException("InternalError check code");
			}
		}
	}

	@SuppressWarnings("unused")
	@Deprecated
	private String retAlTsString(int a, int c, int g, int t, int ref) {
		if (a == 0 && c == 0 && g == 0 && t == 0) {
			throw new IllegalArgumentException("allele number of a,c,g,t is all 0");
		}
		int acgt = 0;
		if (a != 0) {
			acgt += 8;
		}
		if (c != 0) {
			acgt += 4;
		}
		if (g != 0) {
			acgt += 2;
		}
		if (t != 0) {
			acgt += 1;
		}

		// AltsString に refに相当する部分はいらない
		acgt = acgt & (~(0b1000 >> ref));

		if ((acgt & (0b1000 >> ref)) == 0) {
			// System.err.println("No allele (only alts) exsits, a,c,g,t,ref;"
			// +a+" "+c+" "+g+" "+t+" "+ref);
		}

		switch (acgt) {
		case 0b0000:
			return "";
		case 0b1000:
			return "A";
		case 0b0100:
			return "C";
		case 0b0010:
			return "G";
		case 0b0001:
			return "T";
		case 0b1100:
			return "A,C";
		case 0b0110:
			return "C,G";
		case 0b0011:
			return "G,T";
		case 0b1010:
			return "A,G";
		case 0b0101:
			return "C,T";
		case 0b1001:
			return "A,T";
		case 0b1110:
			return "A,C,G";
		case 0b1101:
			return "A,C,T";
		case 0b1011:
			return "A,G,T";
		case 0b0111:
			return "C,G,T";
		default:
			throw new Error("refALTsString , fatal bug when ptinting."
					+ "reference in consensusfile may not correspong with  the content in referenceDBfile");
		}
	}

}

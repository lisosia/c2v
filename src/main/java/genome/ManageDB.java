package genome;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import genome.GenomeDataStoreUtil.PersonalID;
import genome.chr.Chr;
import genome.format.Base;
import genome.format.CheckSex;
import genome.format.ReferenceReader;
import genome.util.PosArrayDecompressor;

final public class ManageDB {

	final AppConfig c;
	final CheckSex checkSex;

	/**
	 * 
	 * @param configFilePath
	 * @param checkSexFilePath
	 *            mergeしたいときはnullが許される
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public ManageDB(String configFilePath, String checkSexFilePath) throws IOException, FileNotFoundException {

		this.c = new AppConfig(configFilePath);

		if (checkSexFilePath == null) {
			this.checkSex = null;
		} else {
			this.checkSex = new CheckSex(checkSexFilePath, this.c.checkSexRatio);
		}

	}

	/*
	 * 100k以上じゃないと処理時間増大, 100k Lineあたりが良さそう == 100k / "穴あき"率 穴あき率 r := posision
	 * 1 ごとに平均 何line存在するか 0<r<1, r~= 0.01 bzip2 compressor's defalut block size
	 * is 900k (can customize 100k ~ 900k) and posisionArrayCompressor write
	 * 4bit per 1line so 900k/4 ~= 200k or greater split_unit_size MAY BE best
	 * And also, too large split_unit causes too-large memory allocation (may
	 * cause OutOfMemoryError )
	 */

	void removeDBData(final PersonalID pid, Chr chr) throws SQLException {
		Connection con = getConnection(pid.getRunID(), chr);
		PreparedStatement ps = con.prepareStatement("remove * from " + c.TABLE_NAME + " where sample_id = ?");
		ps.setString(1, pid.getSampleName());
		ps.executeUpdate();
	}

	public void initDB(String runID, Chr chr) throws ClassNotFoundException, SQLException {

		final String dbPath = getDBFilePath(runID, chr);
		Connection con = DriverManager.getConnection("jdbc:sqlite" + ":" + dbPath);

		Statement statement = con.createStatement();
		statement.setQueryTimeout(c.SQLITE_TIMEOUT_SEC); // set timeout.
		statement.executeUpdate("drop table if exists " + c.TABLE_NAME);
		statement.executeUpdate("create table " + c.TABLE_NAME + " "
		// "(chr integer NOT NULL ," + chrごとで、ファイルで分割することに
				+ "(pos_index integer NOT NULL ," + "sample_id TEXT NOT NULL ," + "pos_array blob NOT NULL ,"
				+ "base_array blob NOT NULL ," + "primary key(sample_id, pos_index) )"); // removed
																							// "chr,"
		statement.executeUpdate("CREATE UNIQUE INDEX uniqindex on base_data(pos_index,sample_id)");

		con.close();
	}

	boolean dbExists(String runID, Chr chr) {
		final String dbPath = getDBFilePath(runID, chr);
		boolean ret = new File(dbPath).isFile();
		return ret;
	}

	String getDBFilePath(String runID, Chr chr) {
		return c.DATA_DIR + runID + ".chr" + chr.getStr();
	}

	public Connection getConnection(String runID, Chr chr) throws SQLException {
		if( !dbExists(runID, chr)) {
			//TODO app-specific Exeptoin may be betther
			throw new RuntimeException("db file not found;path="+getDBFilePath(runID, chr));
		}
		final String dbPath = getDBFilePath(runID, chr);
		Connection con = DriverManager.getConnection("jdbc:sqlite" + ":" + dbPath);
		return con;
	}

	/*
	 * ↑ store ↓ print, Merge
	 */
	/**
	 * 動作遅いかもしれない
	 */
	SortedSet<Integer> getExistingPosIndex(Chr chr, Set<String> runIDs)
			throws ClassNotFoundException, SQLException {
		String get_pos_indexs = "select distinct pos_index from " + c.TABLE_NAME;
		SortedSet<Integer> ret = new TreeSet<Integer>();
		for (String runID : runIDs) {
			if (!dbExists(runID, chr)) {
				throw new Error("DBfile for runID,chr:" + runID + "," + chr + " does not exist.");
			}
			Connection con = getConnection(runID, chr);
			// con.setReadOnly(true);
			// TODO cannot set readonly flag after establishing a connection use
			// sqliteConfig#setReadOnly
			// and SQLiteConfig.createConnection().
			PreparedStatement ps = con.prepareStatement(get_pos_indexs);
			ps.setQueryTimeout(c.SQLITE_TIMEOUT_SEC);
			// ps.setInt(1, chr.getNumForDB() ); REMOVED
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				ret.add(rs.getInt("pos_index"));
			}
		}
		if (ret.size() == 0) {
			throw new IllegalArgumentException(
					"runIDs(chr=" + chr + ") has no record! check DB file. " + "terminate this process");
		}
		return ret;
	}

	// TODO これでよいのか
	/**
	 * Dataがすでに存在するか確かめる.
	 * 
	 * @param con
	 * @param runID
	 * @param sampleID
	 * @return 存在するならtrue
	 * @throws SQLException
	 */
	boolean checkDataExistance(Connection con, String runID, String sampleID) throws SQLException {
		String sql = "select * from " + c.TABLE_NAME + " where sample_id = ?";
		// Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setQueryTimeout(c.SQLITE_TIMEOUT_SEC);
		ps.setString(1, sampleID);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}

	boolean checkDataExistance(Connection con, String runID, String sampleID, Chr chr) throws SQLException {
		String sql = "select * from " + c.TABLE_NAME + " where sample_id = ? ";
		// Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setQueryTimeout(c.SQLITE_TIMEOUT_SEC);
		// ps.setInt(1, chr.getNumForDB() );
		ps.setString(1, sampleID);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}

	/**
	 * For debug
	 **/
	public void printOneSample(String runID, String sampleID, Chr chr)
			throws SQLException, ClassNotFoundException, IOException {

		final String dbPath = getDBFilePath(runID, chr);
		if (!(new File(dbPath)).exists()) {
			System.err.println("such DB file doues not exist!");
		}
		// PersonalID pid = new PersonalID(runID,sampleID);

		String sql = "select * from " + c.TABLE_NAME + " where sample_id = ?";
		
		Connection con = getConnection(runID, chr);
		PreparedStatement ps = con.prepareStatement(sql);
		// ps.setInt(1, chr_to_print.getNumForDB() );
		ps.setString(1, sampleID);
		ResultSet rs = ps.executeQuery();

		int count = 0;
		while (rs.next()) {
			count++;
			PosArrayDecompressor pos_buf = new PosArrayDecompressor(rs.getBytes("pos_array"));
			Base.BaseArrayDeCompressor base_buf = new Base.BaseArrayDeCompressor(rs.getBytes("base_array"));
			int[] base_ret = new int[2];
			int ret_pos;
			while (base_buf.readNextBaseDiff(base_ret) != -1 && (ret_pos = pos_buf.readNextPos()) != -1) {
				if (!chr.getStr().equals("Y") && (base_ret[0] != 0 || base_ret[1] != 0)) {
					System.out.print("! ");
				}
				if (chr.getStr().equals("Y") && (base_ret[1] != 0)) {
					System.out.print("! ");
				}
				
				System.out.printf("%d:[%d,%d]\n", ret_pos, base_ret[0], base_ret[1]);
			}

		}
		if (count == 0) {
			System.err.println("No record in" + c.TABLE_NAME + ":chr" + chr + "" + runID + ":" + sampleID);
		}
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

			// printかつalt==0 -> listを症らくできる
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

	static private class BaseCounter {
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

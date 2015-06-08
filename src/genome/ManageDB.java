package genome;

import genome.GenomeDataStoreUtil.BaseArrayDeCompressor;
import genome.GenomeDataStoreUtil.PersonalGenomeDataCompressor;
import genome.GenomeDataStoreUtil.PersonalGenomeDataDecompressor;
import genome.GenomeDataStoreUtil.PersonalID;
import genome.GenomeDataStoreUtil.PositionArrayDeCompressor;
import genome.chr.Sex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

final public class ManageDB {// Util Class

	final private String DATA_DIR; // = System.getProperty("user.dir") +
									// "/etc/data/";
	final private String TABLE_NAME = "base_data";
	final int DATA_SPLIT_UNIT; // = 100000 * 100; //100k * 100
	final double minimumQual;
	final int minimumDP;
	final String referenceDBPath;
	CheckSex checkSex;
	
	public ManageDB(String configFilePath, String checkSexFilePath) throws IOException{
		this(configFilePath);
		this.checkSex = new CheckSex(checkSexFilePath);
	}
	public ManageDB(String configFilePath) throws IOException,FileNotFoundException{
		// Read config file
		final String CONFIG_FILENAME = configFilePath; // System.getProperty("user.dir")
														// + "/.config";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(CONFIG_FILENAME));
			String data_dir_tmp = br.readLine();
			DATA_DIR = data_dir_tmp.endsWith("/") ? data_dir_tmp : data_dir_tmp
					+ "/";
			DATA_SPLIT_UNIT = Integer.parseInt(br.readLine());
			minimumQual = Double.parseDouble(br.readLine());
			minimumDP = Integer.parseInt(br.readLine());
			referenceDBPath = br.readLine();

			System.err.println(">>> read configfile: " + CONFIG_FILENAME
					+ "\noutputDBfileDir: " + DATA_DIR + "\nDATA_SPLIT_UNIT: "
					+ DATA_SPLIT_UNIT
					+ "\n<minimumQV(double), minimumDP(int)>:" + "<"
					+ minimumQual + ", " + minimumDP + ">"
					+ "\ngenomeReference file path: " + referenceDBPath + "\n");
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				throw new RuntimeException(
						"IOException while closing .configfile", e);
			}
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

	public void store(String runID, String sampleID, final int chr, String filename)
			throws ClassNotFoundException, SQLException, IOException {
		if (dbExist(runID)) {
			System.err.println("database already exists, start storing");
		} else {
			System.err.println("database not exist. start creating db.");
			initDB(runID);
		}

		System.err.println("Storing...[runID:" + runID + ",sampleID:"
				+ sampleID + ",chr:" + chr + "]");
		long t0 = System.nanoTime();
		parseAndStoreDB(new PersonalID(runID, sampleID), chr, filename);
		long t1 = System.nanoTime();
		System.err.println("Store fin, " + (t1 - t0)
				/ (1000 * 1000 * 1000 + 0.0) + " sec passed");

	}

	private boolean dbExist(String runID) {
		return (new File(DATA_DIR + runID).exists());
	}

	private void parseAndStoreDB(PersonalID pid, final int chr, String filename)
			throws SQLException, ClassNotFoundException, IOException {
		// TODO primary制約にひっかかってエラーが出たら、それをキャッチしてクライアントに伝える

		Connection con = getConnection(pid.getRunID());
		if (checkDataExistance(con, pid.getRunID(), pid.getSampleName())) {
			System.err.println("Data[ runID:" + pid.getRunID() + ",sampleName:"
					+ pid.getSampleName() + "] " + "already exists. check <"
					+ DATA_DIR + pid.getRunID() + "> .");
			System.err.println("");
			System.exit(1);
		}

		int pos_index_forDB = 0;
		PersonalGenomeDataCompressor cmpBuf = new PersonalGenomeDataCompressor(
				pid, con, chr, pos_index_forDB);

		boolean isFirst = true;

		// int line_ct_per_spilit = 0;

		ConsensusReader.ConsensusLineInfo lineInfo = new ConsensusReader.ConsensusLineInfo(
				minimumQual, minimumDP);
		Sex sampleSex = checkSex.getSex(pid.getSampleName());
		ConsensusReader consensusReader = new ConsensusReader(filename,sampleSex, chr );

		while (true) {
			if (consensusReader.readFilteredLine(lineInfo) == false) { // 読み込みここまで
				// final STORE
				cmpBuf.StoreDB(TABLE_NAME);
				System.err.println("store finished");
				break;
			}

			if (lineInfo.chr != chr) {
				throw new IllegalArgumentException(
						"与えられたchromesomeと 入力<filenameの中身> が一致しません");
			}

			if (isFirst) {
				isFirst = false;
			}

			if (lineInfo.position >= pos_index_forDB + DATA_SPLIT_UNIT) { // data
																			// spilit
				// line_ct_per_spilit = 0;
				// STORING
				cmpBuf.StoreDB(TABLE_NAME);
				// after store, should reset buffers, and update pos_index
				isFirst = true;
				pos_index_forDB += DATA_SPLIT_UNIT;
				cmpBuf.resetBuffer(pos_index_forDB);
			}

			// line_ct_per_spilit++;
			// WRITE
			//TODO
			if( (chr != 23 && chr != 24) ||
				 sampleSex == Sex.Female ||
				 chr == 23 && ConsensusReader.isPAR_X(lineInfo.position)
				 //ACGT １つだが、Yの文も含めて２つ分数える
			  ) {
				cmpBuf.writeData(lineInfo.position,
						lineInfo.altsComparedToRef[0],
						lineInfo.altsComparedToRef[1]);
			} else if( !( chr == 24 && ConsensusReader.isPAR_Y(lineInfo.position) ) && //つまり男性,XY,非PAR
					 	!lineInfo.genoType.equals("0/1")    ) { // 0/1のときはmisscall, readFilteredLineしてるのでこの行入らないけど一応
				// 0b1111 はMerge時に無視される
				cmpBuf.writeData(lineInfo.position, 
					0b1111,
					lineInfo.altsComparedToRef[0] );
			}else { // 染色体Yで(Maleで)PARのときは何もしない.
			}

		}

		con.close();

	}
	
	
	private Connection initDB(String runID) throws ClassNotFoundException,
			SQLException {

		final String dbPath = DATA_DIR + runID;
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			System.err.println("JDBC Driver NOT FOUND");
			throw e;
		}
		Connection connection = null;
		// create a database connection
		connection = DriverManager.getConnection("jdbc:sqlite" + ":" + dbPath);
		Statement statement = connection.createStatement();
		statement.setQueryTimeout(30); // set timeout to 30 sec.
		statement.executeUpdate("drop table if exists " + TABLE_NAME);
		statement.executeUpdate("create table " + TABLE_NAME + " "
				+ "(chr integer NOT NULL ," + "pos_index integer NOT NULL ,"
				+ "sample_id TEXT NOT NULL ," + "pos_array blob NOT NULL ,"
				+ "base_array blob NOT NULL ,"
				+ "primary key(sample_id, chr, pos_index) )");
		return connection;
	}

	boolean DBexists(String runID) {
		final String dbPath = DATA_DIR + runID;
		boolean ret = new File(dbPath).exists();
		return ret;
	}

	Connection getConnection(String runID) throws SQLException,
			ClassNotFoundException {
		final String dbPath = DATA_DIR + runID;
		Connection con = DriverManager.getConnection("jdbc:sqlite" + ":"
				+ dbPath);
		return con;
	}

	@SuppressWarnings("unused")
	private void storeDB(PreparedStatement ps, String sample_id, int chr,
			int pos_index, byte[] pos_array, byte[] base_array)
			throws SQLException {
		ps.setInt(1, chr);
		ps.setInt(2, pos_index);
		ps.setString(3, sample_id);
		ps.setBytes(4, pos_array);
		ps.setBytes(5, base_array);
		ps.executeUpdate();
	}

	/*
	 * ↑ store ↓ print, Merge
	 */
	/**
	 * 動作遅いかもしれない
	 */
	private SortedSet<Integer> getExistingPosIndex(int chr, Set<String> runIDs)
			throws ClassNotFoundException, SQLException {
		String get_pos_indexs = "select distinct pos_index from " + TABLE_NAME
				+ " where chr = ?";
		SortedSet<Integer> ret = new TreeSet<Integer>();
		for (String runID : runIDs) {
			if (!DBexists(runID)) {
				throw new Error("DBfile for runID:" + runID
						+ " does not exist.");
			}
			Connection con = getConnection(runID);
			PreparedStatement ps = con.prepareStatement(get_pos_indexs);
			ps.setInt(1, chr);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				ret.add(rs.getInt("pos_index"));
			}
		}
		if (ret.size() == 0) {
			throw new IllegalArgumentException("runIDs(chr=" + chr
					+ ") has no record! check inputs/DB. "
					+ "terminate process");
		}
		return ret;
	}

	public void printDiffByChr(String chr, Map<String, ArrayList<String>> id,
			PrintStream out) throws ClassNotFoundException, SQLException,
			IOException {
		int chr_num = (chr.equals("X")) ? 23 : (chr.equals("Y")) ? 24 : Integer
				.valueOf(chr);

		for (int pos : getExistingPosIndex(chr_num, id.keySet())) {
			int[] merged = getMergedData(chr_num, id, pos);
			// String chr_str = (chr==23)? "X" : (chr==24) ? "Y" :
			// String.valueOf(chr);
			new PrintData(chr, referenceDBPath).printMergedData(merged, pos,
					out);
		}
	}

	/**
	 * @param id
	 *            Map<runID, ArrayList of sampleIDs> mergeするData特定用
	 */
	int[] getMergedData(int chr, Map<String, ArrayList<String>> id,
			int pos_index) throws IOException, SQLException,
			ClassNotFoundException {
		int[] ret = new int[4 * DATA_SPLIT_UNIT];
		for (String runID : id.keySet()) {
			int[] mergedByRunID = getMergedDataByRunID(chr, runID,
					id.get(runID), pos_index);
			for (int i = 0; i < ret.length; i++) {
				ret[i] += mergedByRunID[i];
			}
			mergedByRunID = null; // GCを期待
		}
		return ret;
	}

	int[] getMergedDataByRunID(int chr, String runID, List<String> sampleIDs,
			int pos_index) throws SQLException, ClassNotFoundException,
			IOException {
		if (sampleIDs.size() == 0) {
			throw new IllegalArgumentException("sampleIDs[of " + runID
					+ "] 's size == 0");
		}
		StringBuilder sb = new StringBuilder("select * from " + TABLE_NAME
				+ " where chr = ? and pos_index = ? and sample_id in (");

		boolean isFirst = true;
		Connection con = getConnection(runID);
		for (String id : sampleIDs) {
			if (!checkDataExistance(con, runID, id, chr)) {
				throw new Error("[runID,sampleID,chr]:[" + runID + "," + id
						+ "," + chr + "] does not exist! terminate process.");
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
		ps.setInt(1, chr);
		ps.setInt(2, pos_index);
		ResultSet rs = ps.executeQuery();

		int[] ret = new int[4 * DATA_SPLIT_UNIT];
		while (rs.next()) {
			System.err.println("merging   sample_id:"
					+ rs.getString("sample_id") + " pos_index:"
					+ String.valueOf(pos_index));
			PersonalGenomeDataDecompressor d = new PersonalGenomeDataDecompressor(
					rs.getBytes("pos_array"), rs.getBytes("base_array"));
			int[] data = new int[3];
			
			while (d.readNext(data) != -1) {
				final int base1 = data[0];
				final int base2 = data[1];
				final int posRead = data[3];
				if(base1 != 0b1111){
					ret[4 * (posRead - pos_index) + base1] += 1;
				}
				ret[4 * (posRead - pos_index) + base2] += 1;
			}
			
		}

		con.close();
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
	boolean checkDataExistance(Connection con, String runID, String sampleID)
			throws SQLException {
		String sql = "select * from " + TABLE_NAME + " where sample_id = ?";
		// Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, sampleID);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}

	boolean checkDataExistance(Connection con, String runID, String sampleID,
			int chr) throws SQLException {
		String sql = "select * from " + TABLE_NAME
				+ " where chr = ? and sample_id = ? ";
		// Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setInt(1, chr);
		ps.setString(2, sampleID);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}

	/**
	 * For debug
	 **/
	public void printOneSample(String runID, String sampleID, int chr_to_print)
			throws SQLException, ClassNotFoundException, IOException {
		final String dbPath = DATA_DIR + runID;
		if (!(new File(dbPath)).exists()) {
			System.err.println("such DB file doues not exist!");
		}
		// PersonalID pid = new PersonalID(runID,sampleID);

		String sql = "select * from " + TABLE_NAME
				+ " where chr = ? and sample_id = ?";
		Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setInt(1, chr_to_print);
		ps.setString(2, sampleID);
		ResultSet rs = ps.executeQuery();

		int count = 0;
		while (rs.next()) {
			count++;
			PositionArrayDeCompressor pos_buf = new PositionArrayDeCompressor(
					rs.getBytes("pos_array"));
			BaseArrayDeCompressor base_buf = new BaseArrayDeCompressor(
					rs.getBytes("base_array"));
			int[] base_ret = new int[2];
			int ret_pos;
			while (base_buf.readNextBaseDiff(base_ret) != -1
					&& (ret_pos = pos_buf.readNextPos()) != -1) {
				System.out.println("  ------   ");
				System.out.println(ret_pos);
				System.out.println(base_ret[0]);
				System.out.println(base_ret[1]);
			}

		}
		if (count == 0) {
			System.err.println("No record in" + TABLE_NAME + ":chr"
					+ chr_to_print + "" + runID + ":" + sampleID);
		}
	}

}

class PrintData {
	private String chr;
	ReferenceReader rr;

	PrintData(String chr, String refDBPath) throws IOException, SQLException {
		this.chr = chr;
		// TODO WHEN CHR = X/Y
		rr = new ReferenceReader(chr, refDBPath);
	}

	void printMergedData(int[] merged, int pos_index, PrintStream out)
			throws IOException, SQLException {
		if (merged.length % 4 != 0) {
			throw new IllegalArgumentException(
					"arg<merged> 's format is incorrect");
		}
		for (int i = 0; i < merged.length / 4; ++i) {
			int dx = i * 4;
			int absolutePos;
			int alt_A, alt_C, alt_G, alt_T, alt_total;
			if (merged[dx + 1] != 0 || merged[dx + 2] != 0
					|| merged[dx + 3] != 0) {

				absolutePos = pos_index + i;
				final String[] ACGT = { "A", "C", "G", "T" };
				int ref_num = rr.readByNumber(absolutePos);
				if (ref_num != -1) {
					alt_A = merged[dx + (4 - ref_num) % 4];
					alt_C = merged[dx + (4 - ref_num + 1) % 4];
					alt_G = merged[dx + (4 - ref_num + 2) % 4];
					alt_T = merged[dx + (4 - ref_num + 3) % 4];
					alt_total = alt_A + alt_C + alt_G + alt_T;
					// String ALTs =:
					String INFO1 = "AN=" + (alt_total) + ";AC=" + alt_A + ","
							+ alt_C + "," + alt_G + "," + alt_T + ";";
					out.printf("chr%s\t%s\t%s\t.\t%s", chr, ACGT[ref_num],
							retAlTsString(alt_A, alt_C, alt_G, alt_T, ref_num),
							INFO1);
				} else {
					// TODO can't compare
					throw new Error("FETAL check source-code");
				}

			}
		}
	}

	private String retAlTsString(int a, int c, int g, int t, int ref) {
		if (a == 0 && c == 0 && g == 0 && t == 0) {
			throw new IllegalArgumentException(
					"allele number of a,c,g,t is all 0");
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
		if ((acgt & (0b1000 >> ref)) != 0b0000) {
			acgt -= 0b1000 >> (ref);
		} else {
			throw new Error(
					"refALTsString , fatal bug when ptinting.\n"
							+ "reference in consensusfile may not correspong with  the content in referenceDBfile");
		}
		switch (acgt) {
		case 0b0000:return "";
		case 0b1000:return "A";
		case 0b0100:return "C";
		case 0b0010:return "G";
		case 0b0001:return "T";
		case 0b1100:return "A,C";
		case 0b0110:return "C,G";
		case 0b0011:return "G,T";
		case 0b1010:return "A,G";
		case 0b0101:return "C,T";
		case 0b1001:return "A,T";
		case 0b1110:return "A,C,G";
		case 0b1101:return "A,C,T";
		case 0b1011:return "A,G,T";
		case 0b0111:return "C,G,T";
		default:
			throw new Error(
					"refALTsString , fatal bug when ptinting."
							+ "reference in consensusfile may not correspong with  the content in referenceDBfile");
		}
	}

}

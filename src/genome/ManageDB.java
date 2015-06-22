package genome;

import genome.GenomeDataStoreUtil.BaseArrayDeCompressor;
import genome.GenomeDataStoreUtil.PersonalGenomeDataCompressor;
import genome.GenomeDataStoreUtil.PersonalGenomeDataDecompressor;
import genome.GenomeDataStoreUtil.PersonalID;
import genome.GenomeDataStoreUtil.PositionArrayDeCompressor;
import genome.chr.Chr;
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
	
	private static final int SQLITE_TIMEOUT_SEC = 100 * 60;
	final private String DATA_DIR; // = System.getProperty("user.dir") +
									// "/etc/data/";
	final private String TABLE_NAME = "base_data";
	final int DATA_SPLIT_UNIT; // = 100000 * 100; //100k * 100
	final double minimumQual;
	final int minimumDP;
	final String referenceDBPath;
	CheckSex checkSex;

	/**
	 * 
	 * @param configFilePath
	 * @param checkSexFilePath mergeしたいときはnullが許される
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public ManageDB(String configFilePath , String checkSexFilePath) throws IOException,FileNotFoundException{
		if(checkSexFilePath==null){
			this.checkSex = null;
		} else {
			this.checkSex = new CheckSex(checkSexFilePath);
		}
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

			/*System.err.println(">>> read configfile: " + CONFIG_FILENAME
					+ "\noutputDBfileDir: " + DATA_DIR + "\nDATA_SPLIT_UNIT: "
					+ DATA_SPLIT_UNIT
					+ "\n<minimumQV(double), minimumDP(int)>:" + "<"
					+ minimumQual + ", " + minimumDP + ">"
					+ "\ngenomeReference file path: " + referenceDBPath + "\n");*/
		} finally {
			try {
				//TODO
				if(br!=null){ br.close(); }
			} catch (IOException e) {
				e.printStackTrace();
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

	public void store(String runID, String sampleID, final Chr chr, String filename)
	{
		if (dbExists(runID, chr)) {
			// System.err.println("database already exists, start storing");
		} else {
			System.err.println("database not exist. start creating db.");
			try {
				initDB(runID, chr);
			}catch(Exception e){
				System.err.println("Error Init DB");
				System.err.println("ExitProgram before processing");
				System.exit(1);
			}
		}

		System.err.println("Storing...[runID:" + runID + ",sampleID:"
				+ sampleID + ",chr:" + chr + "]");
		long t0 = System.nanoTime();
		final PersonalID pid = new PersonalID(runID, sampleID);
		try{
			parseAndStoreDB( pid, chr, filename);
		}catch (Exception e) {
			System.err.print("error when storing runID,sampleID,chr:"
					+ pid.getRunID()+","+pid.getSampleName()+","+chr);
			if (removeDBData(pid, chr) ) {
				System.err.println(" Removed the data");
			} else {
				System.err.println(" Failed to removed the data");				
			}
		}
		long t1 = System.nanoTime();
		System.err.println("Store fin, " + (t1 - t0)
				/ (1000 * 1000 * 1000 + 0.0) + " sec passed");

	}
	
	private boolean removeDBData(final PersonalID pid, Chr chr) 
	{
		try {
			Connection con = getConnection(pid.getRunID(), chr);
			PreparedStatement ps = con.prepareStatement("remove * from " + TABLE_NAME + " where sample_id = ?");
			ps.setString(1, pid.getSampleName());
			ps.executeUpdate();			
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	private void parseAndStoreDB(PersonalID pid, final Chr chr, String filename)
			throws SQLException, ClassNotFoundException, IOException {
		// TODO primary制約にひっかかってエラーが出たら、それをキャッチしてクライアントに伝える

		Connection con = getConnection(pid.getRunID(), chr);
		if (checkDataExistance(con, pid.getRunID(), pid.getSampleName(), chr)) {
			System.err.println("Data[ runID:" + pid.getRunID() + ",sampleName:"
					+ pid.getSampleName() +"chr:"+ chr +"] " + "already exists. check <"
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

			if ( !lineInfo.chr.equals( chr.getStr() ) ) {
				throw new IllegalArgumentException(
						"与えられた chr: "+chr.getStr() +" と 入力<filename:"+filename+" の中身> が一致しません\n"
								+ "consensusfile:chr:"+lineInfo.chr);
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
				//一気に DATA_SPLIT_UNIT以上 consensusfileのpositionが"歯抜け"の時もありうるのでこのような処理
				while(lineInfo.position >= pos_index_forDB + DATA_SPLIT_UNIT) {
					pos_index_forDB += DATA_SPLIT_UNIT;
				}
				cmpBuf.resetBuffer(pos_index_forDB);
			}

			// line_ct_per_spilit++;
			// WRITE
			//TODO
			if(lineInfo.altsStr.length() !=1 && lineInfo.altsStr.length() != 3) { continue; }
			if( !chr.isSexChr() ||
				 sampleSex == Sex.Female ||
				 chr.getStr() == "X" && ConsensusReader.isPAR_X(lineInfo.position)
				 //ACGT １つだが、Yの文も含めて２つ分数える
			  ) {
				cmpBuf.writeData(lineInfo.position,
						lineInfo.altsComparedToRef[0],
						lineInfo.altsComparedToRef[1]);
			} else if( !( chr.getStr() == "Y" && ConsensusReader.isPAR_Y(lineInfo.position) ) && //つまり男性,XY,非PAR
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
	
	
	public Void initDB(String runID, Chr chr) throws ClassNotFoundException,
			SQLException {

		final String dbPath = getDBFilePath(runID, chr);
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
		statement.setQueryTimeout(SQLITE_TIMEOUT_SEC); // set timeout.
		statement.executeUpdate("drop table if exists " + TABLE_NAME);
		statement.executeUpdate("create table " + TABLE_NAME + " "
				// "(chr integer NOT NULL ," +  chrごとで、ファイルで分割することに
				+ "(pos_index integer NOT NULL ,"
				+ "sample_id TEXT NOT NULL ," 
				+ "pos_array blob NOT NULL ,"
				+ "base_array blob NOT NULL ,"
				+ "primary key(sample_id, pos_index) )"); // removed "chr,"
		statement.executeUpdate("CREATE UNIQUE INDEX uniqindex on base_data(pos_index,sample_id)");
								
		connection.close();
		return null;
	}

	boolean dbExists(String runID, Chr chr) {
		final String dbPath = getDBFilePath(runID, chr);
		boolean ret = new File(dbPath).exists();
		return ret;
	}
	
	String getDBFilePath(String runID, Chr chr) {
		return DATA_DIR + runID + ".chr" + chr.getStr();
	}

	Connection getConnection(String runID, Chr chr) throws SQLException,
			ClassNotFoundException {
		final String dbPath = getDBFilePath(runID, chr);
		Connection con = DriverManager.getConnection("jdbc:sqlite" + ":"
				+ dbPath);
		return con;
	}

	@SuppressWarnings("unused")
	private void storeDB(PreparedStatement ps, String sample_id, int chr,
			int pos_index, byte[] pos_array, byte[] base_array)
			throws SQLException {
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		//ps.setInt(1, chr);
		ps.setInt(1, pos_index);
		ps.setString(2, sample_id);
		ps.setBytes(3, pos_array);
		ps.setBytes(4, base_array);
		ps.executeUpdate();
	}

	/*
	 * ↑ store ↓ print, Merge
	 */
	/**
	 * 動作遅いかもしれない
	 */
	private SortedSet<Integer> getExistingPosIndex(Chr chr, Set<String> runIDs)
			throws ClassNotFoundException, SQLException {
		String get_pos_indexs = "select distinct pos_index from " + TABLE_NAME;
		SortedSet<Integer> ret = new TreeSet<Integer>();
		for (String runID : runIDs) {
			if (!dbExists(runID, chr)) {
				throw new Error("DBfile for runID,chr:" + runID +"," +chr
						+ " does not exist.");
			}
			Connection con = getConnection(runID, chr);
			// con.setReadOnly(true);
			//TODO cannot set readonly flag after establishing a connection use sqliteConfig#setReadOnly
			// and SQLiteConfig.createConnection().
			PreparedStatement ps = con.prepareStatement(get_pos_indexs);
			ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
			// ps.setInt(1, chr.getNumForDB() );   REMOVED
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

	public void printDiffByChr(Chr chr, Map<String, ArrayList<String>> id,
			PrintStream out) throws ClassNotFoundException, SQLException,
			IOException {

		for (int pos : getExistingPosIndex(chr, id.keySet())) {
			int[] merged = getMergedData(chr, id, pos);
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
	int[] getMergedData(Chr chr, Map<String, ArrayList<String>> id,
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

	int[] getMergedDataByRunID(Chr chr, String runID, List<String> sampleIDs,
			int pos_index) throws SQLException, ClassNotFoundException,
			IOException {
		if (sampleIDs.size() == 0) {
			throw new IllegalArgumentException("sampleIDs[of " + runID
					+ "] 's size == 0");
		}
		StringBuilder sb = new StringBuilder("select * from " + TABLE_NAME
				+ " where pos_index = ? and sample_id in (");

		boolean isFirst = true;
		Connection con = getConnection(runID, chr);
		//con.setReadOnly(true);
		
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
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		//ps.setInt(1, chr.getNumForDB() ); REMOVED
		ps.setInt(1, pos_index);
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
				final int posRead = data[2];
				try{
					
				if(base1 != 0b1111){
					ret[4 * (posRead - pos_index) + base1] += 1;
				}
				ret[4 * (posRead - pos_index) + base2] += 1;
				
				}catch (ArrayIndexOutOfBoundsException e){
					System.err.println("pos_index,posread,base1,base2:"+pos_index+","
							+posRead+","+base1+","+base2);
					throw new RuntimeException("array index outofBounds",e);
				}
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
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		ps.setString(1, sampleID);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}

	boolean checkDataExistance(Connection con, String runID, String sampleID,
			Chr chr) throws SQLException {
		String sql = "select * from " + TABLE_NAME
				+ " where sample_id = ? ";
		// Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		//ps.setInt(1, chr.getNumForDB() );
		ps.setString(1, sampleID);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}

	/**
	 * For debug
	 **/
	public void printOneSample(String runID, String sampleID, Chr chr_to_print)
			throws SQLException, ClassNotFoundException, IOException {
		final String dbPath = DATA_DIR + runID;
		if (!(new File(dbPath)).exists()) {
			System.err.println("such DB file doues not exist!");
		}
		// PersonalID pid = new PersonalID(runID,sampleID);

		String sql = "select * from " + TABLE_NAME
				+ " where sample_id = ?"; // removed "chr = ? and" 
		Connection con = getConnection(runID, chr_to_print);
		PreparedStatement ps = con.prepareStatement(sql);
		//ps.setInt(1, chr_to_print.getNumForDB() );
		ps.setString(1, sampleID);
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
	private Chr chr;
	ReferenceReader rr;

	PrintData(Chr chr, String refDBPath) throws IOException, SQLException {
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
				assert ref_num != -1;
				if (ref_num != -1) {
					alt_A = merged[dx + (4 - ref_num) % 4];
					alt_C = merged[dx + (4 - ref_num + 1) % 4];
					alt_G = merged[dx + (4 - ref_num + 2) % 4];
					alt_T = merged[dx + (4 - ref_num + 3) % 4];
					alt_total = alt_A + alt_C + alt_G + alt_T;
					// String ALTs =:
					String INFO1 = "AN=" + (alt_total) + ";AC=" + alt_A + ","
							+ alt_C + "," + alt_G + "," + alt_T + ";";
					//out.println(";" +absolutePos);
					out.printf("chr%s\t%d\t%s\t%s\t.\t%s\n", chr.getStr() ,absolutePos , ACGT[ref_num],
							retAlTsString(alt_A, alt_C, alt_G, alt_T, ref_num),
							INFO1);
				} else {
					// TODO can't compare
					// SKIP!!!!!!!!!!!!!!!!
					/*
					out.printf("refnum %d, absoltePos %d \n",ref_num,absolutePos);
					throw new Error("FETAL check source-code");
					*/
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
		
		// AltsString に refに相当する部分はいらない
		acgt = acgt & (~(0b1000 >> ref));
		
		if ( (acgt & (0b1000 >> ref)) == 0 )  {
			// System.err.println("No allele (only alts) exsits, a,c,g,t,ref;" +a+" "+c+" "+g+" "+t+" "+ref);
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

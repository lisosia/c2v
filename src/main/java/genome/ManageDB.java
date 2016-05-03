package genome;

import static genome.util.Utils.error2StackTrace;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import genome.GenomeDataStoreUtil.PersonalGenomeDataCompressor;
import genome.GenomeDataStoreUtil.PersonalGenomeDataDecompressor;
import genome.GenomeDataStoreUtil.PersonalID;
import genome.ManageDB.MergedData;
import genome.chr.Chr;
import genome.chr.Sex;
import genome.format.Base;
import genome.format.CheckSex;
import genome.format.ConsensusReader;
import genome.format.ReferenceReader;
import genome.util.PosArrayDecompressor;

final public class ManageDB {// Util Class

	private static final int SQLITE_TIMEOUT_SEC = 100 * 60;
	final private String DATA_DIR; // = System.getProperty("user.dir") +
									// "/etc/data/";
	final private String TABLE_NAME = "base_data";
	final int DATA_SPLIT_UNIT; // = 100000 * 100; //100k * 100
	final double minimumQual;
	final int minimumDP;
	final String referenceDBPath;
	final double checkSexRatio;
	CheckSex checkSex;

	/**
	 * 
	 * @param configFilePath
	 * @param checkSexFilePath
	 *            mergeしたいときはnullが許される
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public ManageDB(String configFilePath, String checkSexFilePath) throws IOException, FileNotFoundException {

		// Read config file
		final String CONFIG_FILENAME = configFilePath; // System.getProperty("user.dir")
														// + "/.config";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(CONFIG_FILENAME));
			String data_dir_tmp = br.readLine();
			DATA_DIR = data_dir_tmp.endsWith("/") ? data_dir_tmp : data_dir_tmp + "/";
			DATA_SPLIT_UNIT = Integer.parseInt(br.readLine());
			minimumQual = Double.parseDouble(br.readLine());
			minimumDP = Integer.parseInt(br.readLine());
			referenceDBPath = br.readLine();
			checkSexRatio = Double.parseDouble(br.readLine());
			/*
			 * System.err.println(">>> read configfile: " + CONFIG_FILENAME +
			 * "\noutputDBfileDir: " + DATA_DIR + "\nDATA_SPLIT_UNIT: " +
			 * DATA_SPLIT_UNIT + "\n<minimumQV(double), minimumDP(int)>:" + "<"
			 * + minimumQual + ", " + minimumDP + ">" +
			 * "\ngenomeReference file path: " + referenceDBPath + "\n");
			 */
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("IOException while closing .configfile", e);
			}
		}

		if (checkSexFilePath == null) {
			this.checkSex = null;
		} else {
			this.checkSex = new CheckSex(checkSexFilePath, this.checkSexRatio);
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

	public void store(String runID, String sampleID, final Chr chr, String filename) {
		if (dbExists(runID, chr)) {
			// System.err.println("database already exists, start storing");
		} else {
			System.err.println("database not exist. start creating db.");
			try {
				initDB(runID, chr);
			} catch (ClassNotFoundException | SQLException e) {
				throw new RuntimeException("error while initting db", e);
			}
		}

		System.err.printf("Storing...[runId:%s,sampleId:%s,chr:%s]", runID, sampleID, chr);

		long t0 = System.nanoTime();
		final PersonalID pid = new PersonalID(runID, sampleID);
		try {
			parseAndStoreDB(pid, chr, filename);
		} catch (SQLException | IOException | ClassNotFoundException e) {
			System.err.printf("error while storing runID:%s,sampleID:%s,chr:%s,filename:%s", runID, sampleID, chr,
					filename);
			System.err.printf("printStacktrace:\n%s", error2StackTrace(e));

			try {
				removeDBData(pid, chr);
				System.err.println("Removed the data");
			} catch (SQLException e2) {
				System.err.println(" Failed to removed the data");
				//TODO more detailed info, by onbject<runid,sampleid,chr,filename>
				throw new RuntimeException("store failed ", e2);
			}
			//TODO more detailed info, by onbject<runid,sampleid,chr,filename>
			throw new RuntimeException("store failed, remove the entry <<OBJ>>>", e);
		}
		long t1 = System.nanoTime();
		System.err.println("Store fin, " + (t1 - t0) / (1_000_000_000 + 0.0) + " sec passed. " + runID + ","
				+ sampleID + "," + chr);

	}

	private void removeDBData(final PersonalID pid, Chr chr) throws SQLException {
		Connection con = getConnection(pid.getRunID(), chr);
		PreparedStatement ps = con.prepareStatement("remove * from " + TABLE_NAME + " where sample_id = ?");
		ps.setString(1, pid.getSampleName());
		ps.executeUpdate();
	}

	private void parseAndStoreDB(PersonalID pid, final Chr chr, String filename)
			throws SQLException, ClassNotFoundException, IOException {
		// TODO primary制約にひっかかってエラーが出たら、それをキャッチしてクライアントに伝える

		Connection con = getConnection(pid.getRunID(), chr);
		
		if (checkDataExistance(con, pid.getRunID(), pid.getSampleName(), chr)) {
			String msg = String.format("you tried to store already existing data[pid:%s,chr:%s,filename:%s]", pid,chr,filename);
			msg += String.format("\nCheck sqlite data in %s", new File(DATA_DIR, pid.getRunID() ) );
			throw new RuntimeException(msg);
		}

		int pos_index_forDB = 0;
		PersonalGenomeDataCompressor cmpBuf = new PersonalGenomeDataCompressor(pid, con, chr, pos_index_forDB);

		boolean isFirst = true;

		// int line_ct_per_spilit = 0;

		ConsensusReader.ConsensusLineInfo lineInfo = new ConsensusReader.ConsensusLineInfo(minimumQual, minimumDP);
		Sex sampleSex = checkSex.getSex(pid.getSampleName());
		System.err.println(pid.getSampleName() + "is " + sampleSex.name());
		if (chr.getStr().equals("Y") && sampleSex == Sex.Female) {
			cmpBuf.StoreDB(TABLE_NAME);
			System.err.println("stored empty column bacause of <chrY,female>: " + pid.getSampleName());
			con.close();
			return;
		} // DO nothing
		ConsensusReader consensusReader = new ConsensusReader(filename, sampleSex, chr);

		int store_counter1 = 0;
		int store_counter2 = 0;
		while (true) {
			if (consensusReader.readFilteredLine(lineInfo) == false) { // 読み込みここまで
				// final STORE
				cmpBuf.StoreDB(TABLE_NAME);
				System.err.println("store finished");
				break;
			}
			// System.out.print(lineInfo);

			if (!lineInfo.chr.equals(chr.getStr())) {
				throw new IllegalArgumentException("与えられた chr: " + chr.getStr() + " と 入力<filename:" + filename
						+ " の中身> が一致しません\n" + "consensusfile:chr:" + lineInfo.chr);
			}

			if (isFirst) {
				isFirst = false;
			}

			/*
			 * data spilit (n+1) * DATA_SPLIT_UNIT < (parseしたposition)
			 * のときは、StoreDB そののちにWRITEしている。 よって n*DATA_SPLIT_UNIT < pos =<
			 * (n+1)*DATA_SPLIT_UNIT が分割領域 具体的には
			 * 1-10000000,10000001-20000000,... (positionは1から始まることに注意)
			 */
			if (lineInfo.position > pos_index_forDB + DATA_SPLIT_UNIT) {
				// line_ct_per_spilit = 0;
				// STORING
				cmpBuf.StoreDB(TABLE_NAME);
				// after store, should reset buffers, and update pos_index
				isFirst = true;
				// 一気に DATA_SPLIT_UNIT以上
				// consensusfileのpositionが"歯抜け"の時もありうるのでこのような処理
				// もし歯抜けときは、空の（＝ compBuf.write をしていない） record を書き込む（resetBuffer
				// -> Storeの流れ）
				boolean isFirstLoop = true;
				while (lineInfo.position > pos_index_forDB + DATA_SPLIT_UNIT) {
					if (!isFirstLoop) {
						cmpBuf.StoreDB(TABLE_NAME);
					}
					pos_index_forDB += DATA_SPLIT_UNIT;
					cmpBuf.resetBuffer(pos_index_forDB);
					isFirstLoop = false;
				}
			}

			// line_ct_per_spilit++;
			// WRITE
			// TODO
			if (lineInfo.altsStr.length() != 1 && lineInfo.altsStr.length() != 3) {
				continue;
			}
			if (!chr.isSexChr() || sampleSex == Sex.Female
					|| chr.getStr().equals("X") && ConsensusReader.isPAR_X(lineInfo.position)
			// ACGT １つだが、Yの文も含めて２つ分数える
			) {
				cmpBuf.writeData(lineInfo.position, lineInfo.altsComparedToRef[0], lineInfo.altsComparedToRef[1]);
				store_counter2 += 1;
				/*
				 * System.out.println(" "+(!chr.isSexChr() ) +" "+ (sampleSex ==
				 * Sex.Female) +" "+ (chr.getStr().equals("X") &&
				 * ConsensusReader.isPAR_X(lineInfo.position) ) );
				 */
			} else if (!(chr.getStr().equals("Y") && ConsensusReader.isPAR_Y(lineInfo.position)) && // つまり男性,XY,非PAR
					!lineInfo.genoType.equals("0/1")) { // 0/1のときはmisscall,
														// readFilteredLineしてるのでこの行入らないけど一応
				// 0b1111 はMerge時に無視される
				cmpBuf.writeData(lineInfo.position, 0b1111, lineInfo.altsComparedToRef[0]);
				store_counter1 += 1;
			} else { // 染色体Yで(Maleで)PARのときは何もしない.
				// System.out.println("Do nothing");
			}

		}
		System.err.println("stored column[1,2]:" + store_counter1 + " " + store_counter2);
		con.close();

	}

	public void initDB(String runID, Chr chr) throws ClassNotFoundException, SQLException {

		Connection con = getConnection(runID, chr);
		Statement statement = con.createStatement();
		statement.setQueryTimeout(SQLITE_TIMEOUT_SEC); // set timeout.
		statement.executeUpdate("drop table if exists " + TABLE_NAME);
		statement.executeUpdate("create table " + TABLE_NAME + " "
		// "(chr integer NOT NULL ," + chrごとで、ファイルで分割することに
				+ "(pos_index integer NOT NULL ," + "sample_id TEXT NOT NULL ," + "pos_array blob NOT NULL ,"
				+ "base_array blob NOT NULL ," + "primary key(sample_id, pos_index) )"); // removed
																							// "chr,"
		statement.executeUpdate("CREATE UNIQUE INDEX uniqindex on base_data(pos_index,sample_id)");

		con.close();
	}

	boolean dbExists(String runID, Chr chr) {
		final String dbPath = getDBFilePath(runID, chr);
		boolean ret = new File(dbPath).exists();
		return ret;
	}

	String getDBFilePath(String runID, Chr chr) {
		return DATA_DIR + runID + ".chr" + chr.getStr();
	}

	Connection getConnection(String runID, Chr chr) throws SQLException {
		if( !dbExists(runID, chr)) {
			//TODO app-specific Exeptoin may be betther
			throw new RuntimeException("db file not found;path="+getDBFilePath(runID, chr));
		}
		final String dbPath = getDBFilePath(runID, chr);
		Connection con = DriverManager.getConnection("jdbc:sqlite" + ":" + dbPath);
		return con;
	}

	@SuppressWarnings("unused")
	private void storeDB(PreparedStatement ps, String sample_id, int chr, int pos_index, byte[] pos_array,
			byte[] base_array) throws SQLException {
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		// ps.setInt(1, chr);
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
				throw new Error("DBfile for runID,chr:" + runID + "," + chr + " does not exist.");
			}
			Connection con = getConnection(runID, chr);
			// con.setReadOnly(true);
			// TODO cannot set readonly flag after establishing a connection use
			// sqliteConfig#setReadOnly
			// and SQLiteConfig.createConnection().
			PreparedStatement ps = con.prepareStatement(get_pos_indexs);
			ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
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

	public void printDiffByChr(Chr chr, Map<String, ArrayList<String>> id, PrintStream out, final boolean printNotAlts)
			throws ClassNotFoundException, SQLException, IOException {

		for (int pos : getExistingPosIndex(chr, id.keySet())) {
//			MergedData[] merged = getMergedData(chr, id, pos);
//			System.err.print("init memory:pos : " + pos + "\tsize : " + (MergeArrayFormat.SIZE_PER_BASE * (DATA_SPLIT_UNIT+1)));
			MergedData[] merged = new MergedData[MergeArrayFormat.SIZE_PER_BASE * (DATA_SPLIT_UNIT+1)];
//			System.err.print("...");
//			for(int i=0;i<merged.length;i++){
//				merged[i] = new MergedData();
//			}
//			System.err.println("finish");
			for (String runID : id.keySet()) {
//				System.err.println("run id : " + runID);
				MergedData[] mergedByRunID = getMergedDataByRunID(chr, runID, id.get(runID), pos);
				assert( merged.length == mergedByRunID.length );
				for (int i = 0; i < mergedByRunID.length; i++) {
//					merged[i] += mergedByRunID[i];
					if(mergedByRunID[i] != null){
						if(merged[i] == null){
							merged[i] = new MergedData(mergedByRunID[i].getNum(), mergedByRunID[i].getIdList());
						}
						else{
							merged[i].addData(mergedByRunID[i].getNum(), mergedByRunID[i].getIdList());
						}
					}
				}
//				mergedByRunID = null; // GCを期待
			}
			// String chr_str = (chr==23)? "X" : (chr==24) ? "Y" :
			// String.valueOf(chr);
			new PrintData(chr, referenceDBPath,printNotAlts).printMergedData(merged, pos, out);
//			merged = null; // GCを期待			
		}
	}

//	/**
//	 * @param id
//	 *            Map<runID, ArrayList of sampleIDs> mergeするData特定用
//	 */
//	MergedData[] getMergedData(Chr chr, Map<String, ArrayList<String>> id,
//			int pos_index) throws IOException, SQLException,
//			ClassNotFoundException {
//		MergedData[] ret = new MergedData[MergeArrayFormat.SIZE_PER_BASE * (DATA_SPLIT_UNIT+1)];
//		for(int i=0;i<ret.length;i++){
//			ret[i] = new MergedData();
//		}
//		for (String runID : id.keySet()) {
//			System.err.println("run id : " + runID);
//			MergedData[] mergedByRunID = getMergedDataByRunID(chr, runID, id.get(runID), pos_index);
//			assert( ret.length == mergedByRunID.length );
//			for (int i = 0; i < mergedByRunID.length; i++) {
////				ret[i] += mergedByRunID[i];
//				ret[i].addData(mergedByRunID[i].getNum(), mergedByRunID[i].getIdList());
//			}
//			mergedByRunID = null; // GCを期待
//		}
//		return ret;
//	}

	public class MergedData{
		private int num;
		private StringBuilder idList;

		public MergedData(){
			num = 0;
			idList = new StringBuilder();
		}
		public MergedData(int i){
			num = i;
			idList = new StringBuilder();
		}
		public MergedData(int i, String id){
			num = i;
			idList = new StringBuilder(id);
		}
		public void addNum(int i){
			num += i;
		}
		public int getNum(){
			return num;
		}
		public void addId(String id){
			if(idList.length() != 0){
				idList.append(',');
			}
			idList.append(id);
		}
		public String getIdList(){
			return idList.toString();
		}
		public void addData(int i, String id){
			addNum(i);
			addId(id);
		}
		public String toString(){
			return num + "\t" + idList.toString();
		}
		public void clear(){
			num = 0;
			idList.setLength(0);
		}
	}
	
	MergedData[] getMergedDataByRunID(Chr chr, String runID, List<String> sampleIDs,
			int pos_index) throws SQLException, ClassNotFoundException,
			IOException {
		if (sampleIDs.size() == 0) {
			throw new IllegalArgumentException("sampleIDs[of " + runID + "] 's size == 0");
		}
		if (!dbExists(runID, chr)) {
			throw new IllegalArgumentException("DB does not exist. runID,chr:" + runID + "," + chr);
		}
		StringBuilder sb = new StringBuilder("select * from " + TABLE_NAME + " where pos_index = ? and sample_id in (");

		boolean isFirst = true;
		Connection con = getConnection(runID, chr);
		// con.setReadOnly(true);

		for (String id : sampleIDs) {
			if (!checkDataExistance(con, runID, id, chr)) {
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
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		// ps.setInt(1, chr.getNumForDB() ); REMOVED
		ps.setInt(1, pos_index);
		ResultSet rs = ps.executeQuery();

		// for validation
		HashMap<String, Boolean> gotData = new HashMap<String, Boolean>();
		for (String id : sampleIDs) {
			gotData.put(id, false);
		}
		
		MergedData[] ret = new MergedData[ MergeArrayFormat.SIZE_PER_BASE * (DATA_SPLIT_UNIT+1)];  //AA,AC,...,TT, _A,_B,_C,_T, isALT
//		for(int i=0;i<ret.length;i++){
//			ret[i] = new MergedData();
//		}
//		int gotIdNum = 0;
		while (rs.next()) {
//			gotIdNum += 1;
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
				try{
					final int BASE_DX =  MergeArrayFormat.SIZE_PER_BASE * (posRead - pos_index);
					if( (base1 != 0 && base1!=0b1111) || base2 != 0 ) {
						ret[BASE_DX + MergeArrayFormat.IS_ALT_DX] = new MergedData(1);;//flag for !=ref
					}
					if(base1 != 0b1111){
						if(ret[ BASE_DX + MergeArrayFormat.getSubIndex(base1, base2) ] == null){
							ret[ BASE_DX + MergeArrayFormat.getSubIndex(base1, base2) ] = new MergedData(1, gotID);
						}
						else{
							ret[ BASE_DX + MergeArrayFormat.getSubIndex(base1, base2) ].addData(1, gotID);
						}
						if(ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] == null){
							ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] = new MergedData(2);
						}
						else{
							ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX].addNum(2);						
						}
					} else {
						if(ret[ BASE_DX + MergeArrayFormat.getSubIndex(base1, base2) ] == null){
							ret[ BASE_DX + MergeArrayFormat.getSubIndex(base1, base2) ] = new MergedData(1, gotID);
						}
						else{
							ret[ BASE_DX + MergeArrayFormat.getSubIndex(base1, base2) ].addData(1, gotID);
						}
						if(ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] == null){
							ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] = new MergedData(1);
						}
						else{
							ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX].addNum(1);
						}
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
					if (checkSex.getSex(e.getKey()) == Sex.Female) {
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
		String sql = "select * from " + TABLE_NAME + " where sample_id = ?";
		// Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		ps.setString(1, sampleID);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}

	boolean checkDataExistance(Connection con, String runID, String sampleID, Chr chr) throws SQLException {
		String sql = "select * from " + TABLE_NAME + " where sample_id = ? ";
		// Connection con = getConnection(runID);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setQueryTimeout(SQLITE_TIMEOUT_SEC);
		// ps.setInt(1, chr.getNumForDB() );
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

		String sql = "select * from " + TABLE_NAME + " where sample_id = ?"; // removed
																				// "chr
																				// =
																				// ?
																				// and"
		Connection con = getConnection(runID, chr_to_print);
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
				System.out.println("  ------   ");
				if (!chr_to_print.getStr().equals("Y") && (base_ret[0] != 0 || base_ret[1] != 0)) {
					System.out.print("!");
				}
				if (chr_to_print.getStr().equals("Y") && (base_ret[1] != 0)) {
					System.out.print("!");
				}
				System.out.println(ret_pos);
				System.out.println(base_ret[0]);
				System.out.println(base_ret[1]);
			}

		}
		if (count == 0) {
			System.err.println("No record in" + TABLE_NAME + ":chr" + chr_to_print + "" + runID + ":" + sampleID);
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
	void printMergedData(MergedData[] merged, int pos_index, PrintStream out)
			throws IOException, SQLException {

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

			if( printNotAlts && (merged[base_dx + MergeArrayFormat.TOTAL_AN_DX] == null || merged[base_dx + MergeArrayFormat.TOTAL_AN_DX].getNum() == 0)) {continue;}
			if(!printNotAlts && (merged[base_dx + MergeArrayFormat.IS_ALT_DX] == null || merged[base_dx + MergeArrayFormat.IS_ALT_DX].getNum()   == 0)) {continue;}
			
			// printかつalt==0 -> listを症らくできる
			if ( printNotAlts && (merged[base_dx + MergeArrayFormat.IS_ALT_DX] == null || merged[base_dx + MergeArrayFormat.IS_ALT_DX].getNum() ==0 )) {

				baseCounter.reset();
				absolutePos = pos_index + i;
				int ref_num = rr.readByNumber(absolutePos);
				assert ref_num != -1;

				///////////////////
				// int list[] = { 0 , 4*4+ref_num };
				///////////////////
				int b_num1 = 0;
				String b_id1 = new String();
				if(merged[base_dx + 0] != null){
					b_num1 = merged[base_dx + 0].getNum();
					b_id1 = merged[base_dx + 0].getIdList();
				}
				if(b_num1 !=0) {
					baseCounter.set(0, 0, ref_num, b_num1, b_id1);
				}
				int b_num2 = 0;
				String b_id2 = new String();
				if(merged[base_dx + 4*4] != null){
					b_num2 = merged[base_dx + 4*4].getNum();
					b_id2 = merged[base_dx + 4*4].getIdList();
				}
				if(b_num2 !=0) {
					baseCounter.set(4, 0, ref_num, b_num2, b_id2);
				}
				//////////
				
				final int alt_total = merged[base_dx + MergeArrayFormat.TOTAL_AN_DX].getNum();
				String INFO = "AN=" + (alt_total) + ";GC=" + baseCounter.getGenomeCntStr();

				out.printf("chr%s\t%d\t%s\t%s\t.\t.\t%s\t%s\n", chr.getStr() ,absolutePos , ACGT[ref_num],
						baseCounter.getAltsStr(),
						INFO, baseCounter.getIdLists());
				
				// altなら出力
			} else if( merged[base_dx + MergeArrayFormat.IS_ALT_DX] != null && merged[base_dx + MergeArrayFormat.IS_ALT_DX].getNum() != 0 ) { 
				baseCounter.reset();
				absolutePos = pos_index + i;
				int ref_num = rr.readByNumber(absolutePos);
				assert ref_num != -1;
				int size1 = 5;
				int size2 = 4; // size1*size2 < SIZE_PER_BASE
				for (int l = 0; l < size1 * size2; ++l) {
					// comment-out to print "AA" when ref=="A"
					//if(l==0){continue;}
					int b_num = 0;
					String b_id = new String();
					if(merged[base_dx + l] != null){
						b_num = merged[base_dx + l].getNum();
						b_id = merged[base_dx + l].getIdList();
					}
					int base1 = l / 4;
					int base2 = l % 4;					
					if(b_num !=0) {
						baseCounter.set(base1, base2, ref_num, b_num, b_id);
					}
				}
				final int alt_total = merged[base_dx + MergeArrayFormat.TOTAL_AN_DX].getNum();
				String INFO = "AN=" + (alt_total) + ";GC=" + baseCounter.getGenomeCntStr();

				out.printf("chr%s\t%d\t%s\t%s\t.\t.\t%s\t%s\n", chr.getStr() ,absolutePos , ACGT[ref_num],
						baseCounter.getAltsStr(),
						INFO,baseCounter.getIdLists());

			}
		}
	}

	static private class BaseCounter{
		final private int[] buffer = new int[14]; //aa,ac,ag.at.cc,cg,ct,gg,gt,tt,xA,xC,xG,xT
		final private String[] id_buffer = new String[14];
		String altsStr = null;
		String genomeCntStr = null;
		String idLists = null;
		BaseCounter() {}
		void reset() { 
			for(int i=0;i<buffer.length;++i){buffer[i] =0;} 
			for(int i=0;i<id_buffer.length;++i){id_buffer[i] = new String();} 			
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
		public void set(int base1, int base2, int ref_num,final int allele_num, String idList) {

			// a little tricky
			base2 = (base2 + ref_num)%4;
			if(base1>=4){ buffer[10 + base2] = allele_num; id_buffer[10 + base2] = idList; return;}
			base1 = (base1 + ref_num)%4;
			
			if(base1>base2) { //change to (base1 <= base2)
				int tmp = base2; base2 = base1; base1 = tmp;
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
			id_buffer[index] = idList;
		}

		private void makeStrs() {
			final StringBuilder sb1 = new StringBuilder();
			final StringBuilder sb2 = new StringBuilder();
			final StringBuilder sb3 = new StringBuilder();			
			for(int i=0;i<buffer.length;++i){
				final int AN = buffer[i];
				if(AN != 0){
					if(sb1.length() !=0) {
						sb1.append(","); sb2.append(","); sb3.append("\t");
					}
					sb1.append( this.getStr(i) );
					sb2.append( Integer.toString( AN ) );
					sb3.append( id_buffer[i]);
				}
			}
			if (sb1.length() == 0) {
				throw new IllegalArgumentException("internal error. no alts found");
			}
			this.altsStr = sb1.toString();
			this.genomeCntStr = sb2.toString();
			this.idLists = sb3.toString();
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
		public String getIdLists(){
			if(this.idLists==null){ makeStrs(); }
			return this.idLists;
		}
		
		private String getStr(final int index){
			final String[] ACGT ={"xA","xC","xG","xT"};
			if(index>=10){ return ACGT[index-10]; }
			switch(index) {
			case 0: return "AA";
			case 1: return "AC";
			case 2: return "AG";
			case 3: return "AT";
			case 4: return "CC";
			case 5: return "CG";
			case 6: return "CT";
			case 7: return "GG";
			case 8: return "GT";
			case 9: return "TT";
			default: throw new IllegalArgumentException("InternalError check code");
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

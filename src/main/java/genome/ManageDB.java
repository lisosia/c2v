package genome;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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


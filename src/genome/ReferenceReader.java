package genome;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

public class ReferenceReader {
	private String chr;
	private static String filedir = "~~~~~~~~~~~~~~~";
	private final Connection con;
	private ResultSet rs;
	private final String sql;
	
	private int start_pos;
	private int end_pos;
	private int last_read_pos;
	private String seq;
	public ReferenceReader(String chromosome) throws SQLException {
		//TODO argument check
		this.chr = chromosome; // chr0 ~~ chr22, chrX, chrY, (chrM)
		sql = "select * from " + "TABLE_NAME" + "where description_id = " + chromosome + " order by start asc";
/*
 * FROM UTGB
 */
//		String sql = SQLExpression.fillTemplate("select start, end, sequence from " + "(select * from description where description= '$1') as description "
//				+ "join sequence on sequence.description_id = description.id " + "where start between $2 and $3 " + "and end > $4 order by start",
//				location.chr, searchStart, end, start);

		con = getREfConnection();
		java.sql.Statement stmt = con.createStatement();
		rs = stmt.executeQuery(sql);
		if( ! rs.next() ){ throw new C2VRuntimeException("get 0 size resultset ");}
		
	}
	
	private void setParams() throws IOException, SQLException{
		this.start_pos = rs.getInt("start");		
		this.end_pos = rs.getInt("end");
		InputStream refStream = new GZIPInputStream(new ByteArrayInputStream( rs.getBytes("sequence") ) );
		byte[] b = new byte[1024];
		int byteRead;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		while ( (byteRead = refStream.read(b) ) != -1 ) {
			bos.write(b, 0, byteRead);
		}
		this.seq = new String( bos.toByteArray() );
		this.last_read_pos = -1;
	}
	
	/**
	 * 
	 * @param posision
	 * @return [ACGT]に対応する[0123], 現在の refStreamに positionが存在しない時は -1
	 */
	private int readFromRef(int posision) throws IOException {
		if( last_read_pos >= posision ) { throw new IllegalArgumentException(
				"readFromRefStream should'd called with incremental arg<position>");}
		if(start_pos > posision || end_pos < posision ){throw new IllegalArgumentException(""
				+ "start_pos<DB> <= position <= end_pos<DB> was not satisfied");}
		return this.seq.indexOf(posision - this.start_pos);
	}
	
	public int read(int posision) throws IOException, SQLException {
		this.last_read_pos = posision;
		
		while( posision > this.end_pos ) {
			if( rs.next() == false ) { 
				throw new IllegalArgumentException(
						" reference (pos=" +String.valueOf(posision) +") does not exist" ); 
			}
			setParams();
		}
		
		return readFromRef(posision);
			
	}			
	
	/**
	 * 
	 * @param chr 1~22の文字列表現　または X or Y
	 * @return
	 */
	String getRefFilename(String chr) {
		return "     fjlsahafoihfs iohsei  .sqlite";
	}
	static Connection getREfConnection() {
		//TODO
		final String dbPath = "     ";
		Connection con;
		try {
			Class.forName("org.sqlite.JDBC");
			con = DriverManager.getConnection("jdbc:sqlite" + ":" + dbPath );
			con.setAutoCommit(false);
		}catch(SQLException e) {
			throw new C2VRuntimeException("ゲノムのリファレンスのDBに接続できません", e);
		}catch(ClassNotFoundException e) {
			throw new C2VRuntimeException("ゲノムのリファレンスのDBに接続できません", e);
		}
		return con;
	}

}

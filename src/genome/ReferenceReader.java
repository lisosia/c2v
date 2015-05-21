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
	private int chr_num;

	private final Connection con;
	private ResultSet rs;
	private final String sql;
	
	private int start_pos;
	private int end_pos;
	private int last_read_pos;
	private String seq;
	private final String ReferenceDBPath;
	/**
	 * 
	 * @param chromosome [ 1-22 | X | Y ].
	 * @throws SQLException
	 */
	public ReferenceReader(String chromosome, String referenceDBpath) throws SQLException {
		//TODO argument check
		this.ReferenceDBPath = referenceDBpath;
		if(chromosome.equals("X")) {this.chr_num = 23; 
		} else if(chromosome.equals("Y")) {this.chr_num=24;
		} else {
			try {
				this.chr_num = Integer.parseInt(chromosome);				
			} catch ( NumberFormatException e) {
				throw new IllegalArgumentException("arg<chromosome> should be [ 1-22 | X | Y ]"
						 + "chromosome: [" + chromosome + "]");
			}
		}
		
		final String REF_TABLENAME = "sequence";
		sql = "select * from " + REF_TABLENAME + "where description_id = " + String.valueOf(this.chr_num)
				+ " order by start asc";
/*
 * FROM UTGB
 */
//		String sql = SQLExpression.fillTemplate("select start, end, sequence from " + "(select * from description where description= '$1') as description "
//				+ "join sequence on sequence.description_id = description.id " + "where start between $2 and $3 " + "and end > $4 order by start",
//				location.chr, searchStart, end, start);

		con = getRefConnection();
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
	 * @param chr 1~22の文字列表現　または "X" or "Y"
	 * @return
	 */
	String getRefFilename(String chr) {
		//TODO
		return this.ReferenceDBPath;
	}
	Connection getRefConnection() {
		//TODO
		final String dbPath = this.ReferenceDBPath;// use getRefFilename()
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

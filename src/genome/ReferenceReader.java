package genome;

import java.beans.Statement;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
	private InputStream refStream;
	public ReferenceReader(String chromosome) throws SQLException {
		//TODO argument check
		this.chr = chromosome;
		sql = "select * from " + "TABLE_NAME" + "where chr = " + chromosome + " order by start asc";
		
		con = getREfConnection();
		java.sql.Statement stmt = con.createStatement();
		rs = stmt.executeQuery(sql);
		if( ! rs.next() ){ throw new C2VRuntimeException("get 0 size resultset ");}
		
	}
	private void setParams() {
		this.start_pos = rs.getInt("start");		
		this.end_pos = rs.getInt("end");
		//TODO
		refStream = new GZIPInputStream(new ByteArrayInputStream( rs.getBytes(columnLabel) ));
	}
	
	/**
	 * 
	 * @param posision
	 * @return [ACGT]に対応する[0123], 現在の refStreamに positionが存在しない時は -1
	 */
	private int readFromRefStream(int posision) {
		
	}
	public int read(int posision) throws SQLException {
		this.last_read_pos = posision;
		
		while( posision > this.end_pos ) {
			if( rs.next() == false ) { 
				throw new IllegalArgumentException(
						" reference (pos=" +String.valueOf(posision) +") does not exist" ); 
			}
			setParams();
		}
		
		return readFromRefStream(posision);
			
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
	/*
	public String getFilename(String chr) {
		if( chr.equals("X") ) {
			return "  ~~~~~~~~~~~~~~~ ";
		} else if ( chr.equals("Y") ) {
			return "                .Y";
		} else {
			int num;
			try {
				num = Integer.valueOf(chr);
			}catch( NumberFormatException  e ) {
				throw new IllegalArgumentException( "引数 chr:[" + chr + "] を解釈できません" );
			}
			if( num < 1 || num > 22  ){  
				throw new IllegalArgumentException("0以下又は22より大きい染色体番号は無効");}
			return "        ~~~~~~~~~~~~~~~~~~~~    ";
		}
	}
	*/
}

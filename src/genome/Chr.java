package genome;

public abstract class Chr {
	
	private final int numForDB;
	private String str = null;
	
	public Chr(final int numForDB) {
		this.numForDB = numForDB;
	};
	public Chr(final String str) {
		this.numForDB = Str2Num(str);
	}	
	
	public int getNumForDB() {
		return numForDB;
	};
	public String getStr() {
		return (this.str==null)? (this.str = Num2Str(this.numForDB)) : this.str;
	};
	
	public abstract Chr[] AllChrs(Sex sex);
	public abstract Chr[] AllChrs();
	public abstract Chr[] NormalChrs();
	public abstract boolean isSexChr();
	public abstract boolean isSexChr(Sex sex);
	
	abstract protected String Num2Str(final int num);
	abstract protected int Str2Num(final String str);
}

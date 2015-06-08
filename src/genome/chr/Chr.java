package genome.chr;

import genome.Sex;



public class Chr {
	private final int numForDB;
	private final String str;
	private final Sex sex;
	public Chr(final int numForDB, final String str) {
		this.numForDB = numForDB;
		this.str = str;
		this.sex = null;
	}
	public int getNumForDB() {
		return numForDB;
	}
	public String getStr() {
		return str;
	}

	public boolean isSexChr() {
		if(this.sex == null) {
			return false;
		} else {
			return true;
		}
	}

	public boolean isSexChr(Sex sex) {
		if(this.sex == null || this.sex != sex) {
			return false;
		} else {
			return true;
		}
	}
	public String toString() {
		return "Chr[str:"+str+"/numForDB:"+numForDB+"]";
	}
}

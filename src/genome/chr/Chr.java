package genome.chr;





public class Chr {
	private final int numForDB;
	private final String str;
	private final boolean isMaleSexChr;
	private final boolean isFemaleSexChr;
	
	public Chr(final int numForDB, final String str) {
		this.numForDB = numForDB;
		this.str = str;
		this.isMaleSexChr = this.isFemaleSexChr = false;
	}

	public Chr(final int numForDB, final String str, final boolean isMaleSexChr, final boolean isFemaleSexChr) {
		this.numForDB = numForDB;
		this.str = str;
		this.isMaleSexChr = isMaleSexChr;
		this.isFemaleSexChr = isFemaleSexChr;
	}
	public int getNumForDB() {
		return numForDB;
	}
	public String getStr() {
		return str;
	}

	public boolean isSexChr(Sex sex) {
		switch (sex) {
		case Male :  return this.isMaleSexChr;
		case Female: return this.isFemaleSexChr;
		default: throw new IllegalArgumentException(); // NEVER REACH HERE
		}
	}
	
	public boolean isSexChr() {
		return (isMaleSexChr || isFemaleSexChr);
	}
	
	public String toString() {
		return "Chr[str:"+str+"/numForDB:"+numForDB+"]";
	}
}

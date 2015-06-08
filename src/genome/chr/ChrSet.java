package genome.chr;

import java.util.ArrayList;


public class ChrSet {
	
	private final String name;
	private final int normalChrSize;
	private final ChrSetArg[] chrSetArgs;
	
	ChrSet(final String name,int normalChrSize, final ChrSetArg... chrSetArgs ) {
		this.name = name;
		this.normalChrSize = normalChrSize;
		this.chrSetArgs = chrSetArgs;
	}
	
	public String getName() {
		return this.name;
	}
	public int getNormalChrSize() {
		return this.normalChrSize;
	}
	
	public java.util.List<Chr> getNormalChrs() {
		java.util.List<Chr> ret = new ArrayList<Chr>(normalChrSize+1);
		for (int i = 1; i <= normalChrSize; i++) {
			ret.add( getChr(i));
		}
		return ret;
	}
	
	public java.util.List<Chr> getSexChrs(final Sex sex) {
		java.util.List<Chr> ret = new ArrayList<Chr>();
		for (int i = 0; i < chrSetArgs.length ; i++) {
			ChrSetArg e = this.chrSetArgs[i];
			Chr toAdd = new Chr(e.numForDB, e.sexChr.getStr(), e.isMaleSexChr,e.isFemaleSexChr );
			if(sex==Sex.Male) {
				if(e.isMaleSexChr)  {ret.add( toAdd );}
			} else {
				if(e.isFemaleSexChr){ret.add( toAdd );}				
			}
		}
		return ret;
	}
	
	public java.util.List<Chr> getSexChrs() {
		java.util.List<Chr> ret = new ArrayList<Chr>();
		for (int i = 0; i < chrSetArgs.length ; i++) {
			ChrSetArg e = this.chrSetArgs[i];
			Chr toAdd = new Chr(e.numForDB, e.sexChr.getStr(), e.isMaleSexChr,e.isFemaleSexChr );
			ret.add( toAdd );
		}
		return ret;
	}

	public Chr getChr(final int numForDB) {
		return new Chr(numForDB,  numToStr(numForDB) );
	}
	
	public Chr getChr(final String str) {
		return new Chr(strToNum(str),  str );
	}
	
	private String numToStr(final int numForDB) {
		if( numForDB >= 1 && numForDB <= normalChrSize ) {
			return Integer.toString(numForDB);
		} else {
			for (int i = 0; i < chrSetArgs.length; i++) {
				if(chrSetArgs[i].numForDB == numForDB) {
					return chrSetArgs[i].sexChr.getStr();
				}
			}
			throw new IllegalArgumentException("numForDB:"+numForDB+" is invalid");
		}
	}

	private int strToNum(final String str) {
		for (int i = 0; i < chrSetArgs.length; i++) {
			if(chrSetArgs[i].sexChr.getStr().equals( str )) {
				return chrSetArgs[i].numForDB;
			}
		}
		try {
			Integer ret = Integer.parseInt(str);
			return ret;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("str:"+str+" is invalid");
		}
	}

	
}

class ChrSetArg {
	final SexChr sexChr;
	final int numForDB;
	final boolean isMaleSexChr;
	final boolean isFemaleSexChr;
	
	ChrSetArg(final SexChr sc, final int numForDB,
				boolean isMaleSexChr, boolean isFemaleSexChr ) {
		this.sexChr = sc;
		this.numForDB = numForDB;
		this.isMaleSexChr = isMaleSexChr;
		this.isFemaleSexChr = isFemaleSexChr;
	}
}
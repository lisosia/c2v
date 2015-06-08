package genome.chr;

import genome.Sex;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class ChrSet {
	
	private final String name;
	private final int normalChrSize;
	private final EnumSet<SexChr> sexChrs;
	private final EnumMap<Sex, EnumSet<SexChr>> sexChrsBySex;
	
	private final Map<String, Integer> strToInt;
	private final Map<Integer,String> intToStr;
	
	ChrSet(final String name,int normalChrSize, final ChrSetArg... chrSetArgs ) {
		this.name = name;
		this.normalChrSize = normalChrSize;
		
		sexChrsBySex = new EnumMap<Sex, EnumSet<SexChr>>(Sex.class);
		sexChrsBySex.put(Sex.Male,   EnumSet.noneOf(SexChr.class) );
		sexChrsBySex.put(Sex.Female, EnumSet.noneOf(SexChr.class) );
		
		sexChrs = EnumSet.noneOf(SexChr.class);
		strToInt = new HashMap<String, Integer>();
		intToStr = new HashMap<Integer,String>();
		for(ChrSetArg e : chrSetArgs) {
			sexChrs.add( e.sexChr );
			strToInt.put( e.sexChr.getStr(), e.numForDB );
			intToStr.put( e.numForDB, e.sexChr.getStr() );
			if( e.isMaleSexChr ) {
				sexChrsBySex.get(Sex.Male  ).add(e.sexChr);
			}
			if( e.isFemaleSexChr ) {
				sexChrsBySex.get(Sex.Female).add(e.sexChr);
			}
		}
	}
	
	public String getName() {
		return this.name;
	}
	public int getNormalChrSize() {
		return this.normalChrSize;
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
			String ret = intToStr.get(numForDB);
			if(ret==null) { throw new IllegalArgumentException("numForDB:"+numForDB+" is invalid"); }
			return ret;
		}
	}

	private int strToNum(final String str) {
		Integer ret = strToInt.get(str);
		if( ret != null ) { return ret; }
		try{ return Integer.parseInt(str);
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
package genome;

import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;
import genome.chr.Sex;

public class TestHumanChrSet {

	public static void main(String[] args) {
		final ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();

		System.err.println("------------Normal---------");
		for (Chr chr: humanChrSet.getNormalChrs() ){
			System.err.println( chr.toString() );
			System.err.println( chr.isSexChr(  ) );
			System.err.println( chr.isSexChr( Sex.Male ) );
			System.err.println( chr.isSexChr( Sex.Female ) );
		}

		System.err.println("-----------SexChrs----------");
		
		for (Chr chr: humanChrSet.getSexChrs() ){
			System.err.println( chr.toString() );
			System.err.println( chr.isSexChr(  ) );
			System.err.println( chr.isSexChr( Sex.Male ) );
			System.err.println( chr.isSexChr( Sex.Female ) );
		}
		
		System.err.println("-----------Male SexChr----------");
		
		for (Chr chr: humanChrSet.getSexChrs(Sex.Male) ){
			System.err.println( chr.toString() );
			System.err.println( chr.isSexChr(  ) );
			System.err.println( chr.isSexChr( Sex.Male ) );
			System.err.println( chr.isSexChr( Sex.Female ) );
		}

		System.err.println("-----------Female SexChr----------");
		
		for (Chr chr: humanChrSet.getSexChrs(Sex.Female) ){
			System.err.println( chr.toString() );
			System.err.println( chr.isSexChr(  ) );
			System.err.println( chr.isSexChr( Sex.Male ) );
			System.err.println( chr.isSexChr( Sex.Female ) );
		}

	}

}

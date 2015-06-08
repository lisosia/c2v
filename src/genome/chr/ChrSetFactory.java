package genome.chr;



public final class ChrSetFactory {
	private ChrSetFactory(){}
	
	public static ChrSet getHumanChrSet() {	
		return new ChrSet("Human", 22,
							new ChrSetArg(SexChr.chrX, 23,true,true), 
							new ChrSetArg(SexChr.chrY, 24,true,false) 
						 );
	}
	
}

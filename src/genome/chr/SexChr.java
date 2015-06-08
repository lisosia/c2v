package genome.chr;

enum SexChr {
	chrX("X"), chrY("Y");
	private String str;
	
	private SexChr(final String strInConsensus) {
		this.str = strInConsensus;
	}
	
	String getStr() {
		return this.str;
	}
}

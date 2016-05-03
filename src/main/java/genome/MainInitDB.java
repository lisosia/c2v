package genome;

import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;

public class MainInitDB {
	public static void main(String[] args) throws Exception{
		if (args.length!=2) {
			throw new IllegalArgumentException("args: config runID");
		}
		final String configFilePath = args[0];
		final String runID = args[1];
		final ManageDB MDB = new ManageDB(configFilePath, null);
		final ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
		
		for(Chr chr: humanChrSet.getNormalChrs()) {
			if(!MDB.dbExists(runID, chr)) {MDB.initDB(runID, chr);}
		}
		for(Chr chr: humanChrSet.getSexChrs() ) {
			if(!MDB.dbExists(runID, chr)) {MDB.initDB(runID, chr);}
		}
			
	}
	
}

package genome;

import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;

import java.io.IOException;
import java.sql.SQLException;

public class MainInitDB {
	public static void main(String[] args) {
		if (args.length!=2) {
			throw new IllegalArgumentException("args1: config, args2 runID");
		}
		final String configFilePath = args[0];
		final String runID = args[1];
		try {
			final ManageDB MDB = new ManageDB(configFilePath, null);
			final ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
			for(Chr chr: humanChrSet.getNormalChrs()) {
				if(!MDB.dbExists(runID, chr)) {MDB.initDB(runID, chr);}
			}
			for(Chr chr: humanChrSet.getSexChrs() ) {
				if(!MDB.dbExists(runID, chr)) {MDB.initDB(runID, chr);}
			}
			
		} catch (ClassNotFoundException | SQLException | IOException e) {
			e.printStackTrace();
		}
	}
	
}

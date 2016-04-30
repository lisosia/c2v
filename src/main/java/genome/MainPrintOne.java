package genome;

import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;

import java.io.IOException;
import java.sql.SQLException;

public class MainPrintOne {


	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {

		final String configFilePath;
		final ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
		if (args.length != 4) {
			throw new IllegalArgumentException(
					"Usage: java -jar jarfile genome.MainPrintOne configPath runID sampleID chr");
		} else {
			configFilePath = args[0];			
		}
		ManageDB mdb = new ManageDB(configFilePath, null);
		mdb.printOneSample(args[1], args[2], humanChrSet.getChr(args[3] ) );				
	}

	

}

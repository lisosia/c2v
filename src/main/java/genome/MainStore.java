package genome;

import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;

public class MainStore {
	/**
	 * 
	 * @param args
	 *            inputConsensusFileName, dbFilterValue,qbFiletrvalue
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final String runID, sampleID, consensuFilePath, configFilePath, checkSexFilePath;
		final Chr chr;
		ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
		if (args.length != 6) {
			throw new IllegalArgumentException(
					"Usage: java -jar jarfile genome.MainStore runDir sampleID consensuFilePath chr_num([1-22]|X|Y) configPath checkSexFilePath");
		} else {
			runID = args[0];
			sampleID = args[1];
			consensuFilePath = args[2];
			chr = humanChrSet.getChr(args[3]);
			configFilePath = args[4];
			checkSexFilePath = args[5];
		}
		ManageDB mdb = new ManageDB(configFilePath, checkSexFilePath);
		try {
			mdb.store(runID, sampleID, chr, consensuFilePath);
		} finally {

		}
	}

}

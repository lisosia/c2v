package genome;

public class MainStore {
	/**
	 * 
	 * @param args
	 *            inputConsensusFileName, dbFilterValue,qbFiletrvalue
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final String runID, sampleID, consensuFilePath, configFilePath, checkSexFilePath;
		final int chr;
		if (args.length != 6) {
			throw new IllegalArgumentException(
					"Usage: java -jar jarfile genome.MainStore runDir sampleID consensuFilePath chr_num(1-24,23=X,24=Y) configPath checkSexFilePath");
		} else {
			runID = args[0];
			sampleID = args[1];
			consensuFilePath = args[2];
			chr = Integer.parseInt(args[3]);
			configFilePath = args[4];
			checkSexFilePath = args[5];
		}
		ManageDB mdb = new ManageDB(configFilePath,checkSexFilePath);
		try {
			mdb.store(runID, sampleID, chr, consensuFilePath);
		} finally {

		}
	}

}

package genome;

public class MainStore {
	/**
	 * 
	 * @param args inputConsensusFileName, dbFilterValue,qbFiletrvalue
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final String runID,sampleID,consensuFilePath;
		final int chr;
		if(args.length != 4) {
			throw new IllegalArgumentException("java javafile runDir sampleID consensuFilePath chr_num(1-24,23=X,24=Y)");
		}else {
			runID = args[0];
			sampleID = args[1];
			consensuFilePath = args[2];
			chr = Integer.parseInt(args[3]);
		}
		ManageDB mdb = new ManageDB();
		try{
				mdb.store(runID, sampleID , chr, consensuFilePath );	
		}finally{
			
		}
	}

	
}


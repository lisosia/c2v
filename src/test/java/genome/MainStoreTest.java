package genome;

import org.junit.Test;

import genome.ManageDB;
import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;

public class MainStoreTest {

	@Test
	public void testStore() throws Exception {
		String filename = getClass().getResource("/consensus/chr1.1").getFile();

		ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
		Chr chr = humanChrSet.getChr(1);
		String runID = "001";
		String sampleID = "0078";
		String configPath = getClass().getResource("/config/config.0").getFile();
		String checkSexPath = getClass().getResource("/checksex/0").getFile();

		ManageDB mdb = new ManageDB(configPath, checkSexPath);
		mdb.store(runID, sampleID, chr, filename);

		mdb.printOneSample(runID, sampleID, chr);

	}

}

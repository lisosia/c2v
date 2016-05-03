package genome;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

import genome.chr.Chr;
import genome.chr.ChrSetFactory;

public class MainMergeTest {

	@Test
	public void testMerge() throws ClassNotFoundException, SQLException, IOException {

		// Should check duplication before merge

		final String runID = "001";
		HashMap<String, ArrayList<String>> id = new HashMap<String, ArrayList<String>>();
		id.put(runID, new ArrayList<String>());
		id.get(runID).add("0078");

		Chr chr = ChrSetFactory.getHumanChrSet().getChr(1);

		System.out.println("print SNP");
		long t0 = System.nanoTime();
		String config = getClass().getResource("/config/config.0").getFile();
		ManageDB mdb = new ManageDB(config, null);
		mdb.printDiffByChr(chr, id, System.out, false);
		long t1 = System.nanoTime();
		System.out.println((t1 - t0) / (1_000_000_000.0) + "sec to merge");

	}
}

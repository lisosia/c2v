package test;

import genome.ManageDB;
import genome.chr.Chr;
import genome.chr.ChrSetFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class TestMerge {
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {

		// Should check duplication before merge

		final String runID = "001";
		HashMap<String, ArrayList<String>> id = new HashMap<String, ArrayList<String>>();
		id.put(runID, new ArrayList<String>());

		for (int i = 0; i < 5; ++i) {
			id.get(runID).add(String.valueOf(i));
		}
		/*
		 * id.put("002", new ArrayList<String>() );
		 * id.get("002").add(String.valueOf
		 * (0));id.get("002").add(String.valueOf(1));
		 */
		Chr chr = ChrSetFactory.getHumanChrSet().getChr(12);
		// ManageDB.printOneSample(runID, "A", 12);

		System.out.println("print SNP");
		long t0 = System.nanoTime();
		String config = "/home/denjo/DOCS/workspace/Consensus2VCF/etc/.config";
		ManageDB mdb = new ManageDB(config, null);
		mdb.printDiffByChr(chr, id,
				new PrintStream(new File("/home/denjo/Documents/workspace/Consensus2VCF/etc/vcf_data/" + runID))
				,false);
		long t1 = System.nanoTime();
		System.out.println((t1 - t0) / (1000 * 1000 * 1000 + 0.0)
				+ "sec to merge");

	}
}

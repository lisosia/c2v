package genome;

import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;

public class MainMerge {

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {

		final String configFilePath, checkSexFilePath;
		final String chrStr;
		final ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
		final boolean printNotAlts;
		if (args.length != 4) {
			throw new IllegalArgumentException(
					"Usage: java -jar jarfile genome.MainMerge [chr([1-22]|X|Y) | 'all'] configPath checksex [true|false](print notAlts or not)");
		} else {
			configFilePath = args[1];
			chrStr = args[0];
			checkSexFilePath = args[2];
			switch (args[3]) {
			case "true": printNotAlts = true; break;
			case "false":printNotAlts = false;break;
			default: throw new IllegalArgumentException("3rd arg should be [true] or [false]");
			}
		}

		Map<String, ArrayList<String>> id = createList(System.in);

		ManageDB mdb = new ManageDB(configFilePath, checkSexFilePath);
		if (chrStr.equals("all") ) {
			for (Chr c : humanChrSet.getNormalChrs() ) {
				mdb.printDiffByChr(c, id, System.out,printNotAlts);				
			}
			for (Chr c : humanChrSet.getSexChrs() ) {
				mdb.printDiffByChr(c, id, System.out,printNotAlts);				
			}

		} else {
			Chr chr = humanChrSet.getChr( args[0] );			
			mdb.printDiffByChr(chr, id, System.out,printNotAlts);
		}

	}

	static Map<String, ArrayList<String>> createList(InputStream in) {
		Scanner scanner = null;
		String runID, sampleID;
		HashMap<String, ArrayList<String>> ret = new HashMap<String, ArrayList<String>>();
		HashSet<String> sampleIDsCheck = new HashSet<String>();
		try {
			scanner = new Scanner(in);
			while (scanner.hasNext()) {
				runID = scanner.next();
				sampleID = scanner.next();
				if (!ret.containsKey(runID)) {
					ret.put(runID, new ArrayList<String>());
				}
				;
				ArrayList<String> list = ret.get(runID);
				if (!sampleIDsCheck.contains(sampleID)) {
					sampleIDsCheck.add(sampleID);
					list.add(sampleID);
				} else {
					throw new Error("sampleID Duplication, check input");
				}
			}
		} finally {
			scanner.close();
		}
		return ret;
	}

}

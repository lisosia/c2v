package genome;

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

		final String configFilePath;
		final String chr;
		if (args.length != 2) {
			throw new IllegalArgumentException(
					"Usage: java -jar jarfile genome.MainMerge [chr_num(1-24,23=X,24=Y) | 'all'] configPath");
		} else {
			configFilePath = args[1];
			chr = args[0];
		}

		Map<String, ArrayList<String>> id = createList(System.in);

		ManageDB mdb = new ManageDB(configFilePath);
		if (chr == "all") {
			for (int chr_num = 1; chr_num <= 22; chr_num++) {
				mdb.printDiffByChr(chr, id, System.out);
			}
			mdb.printDiffByChr("X", id, System.out);
			mdb.printDiffByChr("Y", id, System.out);
		} else {
			mdb.printDiffByChr(chr, id, System.out);
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

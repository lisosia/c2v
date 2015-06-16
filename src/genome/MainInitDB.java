package genome;

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
			if(MDB.DBexists(runID)) {
				System.err.println("DB for runID:"+runID+" already exists.");
				System.exit(-1);
			} else {
				MDB.initDB(runID);
			}
		} catch (ClassNotFoundException | SQLException | IOException e) {
			e.printStackTrace();
		}
	}
	
}

package genome;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;


public class MainMerge {
	public static void main(String[] args) throws ClassNotFoundException,SQLException,IOException {
		final String outputDataDir= args[0];
		final String outputDataPrefix = args[1];
		final String configFilePath;
		//Should check duplication before merge
			
		final String runID = "001";
		HashMap<String, ArrayList<String>> id = new HashMap<String,ArrayList<String>>();
		id.put(runID, new ArrayList<String>());
		
/*		for(int i=0; i<5; ++i){
			id.get(runID).add( String.valueOf(i) );
		}
*/
		int chr = 12;
		ManageDB mdb = new ManageDB(configFilePath);
		mdb.printDiffByChr( chr, id, 
			new PrintStream( new File( outputDataDir+outputDataPrefix + String.valueOf(chr) ))
			);
		
	}
}

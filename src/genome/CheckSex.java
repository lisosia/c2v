package genome;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckSex {
	
	Map<String , Sex> map;
	final String filename;
	
	public CheckSex(String file) throws FileNotFoundException, IOException{
		this.filename = file;
		BufferedReader br = new BufferedReader(new FileReader(file));
		map = new ConcurrentHashMap<String, Sex>();
		String line;
		while ( (line=br.readLine()) != null ) {
			if(line.equals("")) { continue; }
			int e1,e2;
			String[] e = line.split("\t");
			String sampleID = e[0];
			e1 = Integer.parseInt(e[1]);
			e2 = Integer.parseInt(e[2]);
			double ratio = e1/(e1+e2);
			if( ratio < 0.1 ) {
				map.put(sampleID, Sex.Male);
			}else {
				map.put(sampleID, Sex.Female);				
			}
		}
		br.close();
	}	
	
	public Sex getSex(String sampleID) {
		if( map.containsKey(sampleID) ) {
			return map.get(sampleID);
		}else {
			throw new IllegalArgumentException(
					"sampleID:" + sampleID + " 's data not found in file:" + filename);
		}
	}
	
}

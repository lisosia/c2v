package genome;

import genome.chr.Sex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CheckSex {
	
	Map<String , Sex> map;
	final String filename;
	
	public CheckSex(final String filename,final double checkSexRatio) throws FileNotFoundException, IOException{
		this.filename = filename;
		File file = new File(filename);
		if(!file.exists()) { throw new FileNotFoundException("file not fond:"+filename); }
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
			double ratio = e1/(e1+e2+0.0);
			if( ratio < checkSexRatio ) {
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

package genome.chr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public enum Sex {
	Male,Female;
	public static Sex getSex(String ratioFilename, String sampleID){
		BufferedReader br = null;
		int e1,e2;
		try {
			 br = new BufferedReader(new FileReader(ratioFilename));
			 String[] elements;
			 while (true) {
				 elements = br.readLine().split("\\t");
				 if( elements[0].equals(sampleID) ){
					 e1 = Integer.parseInt(elements[1]);
					 e2 = Integer.parseInt(elements[2]);
					 break;
					}
			}
			 
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("can't find the file <" + ratioFilename +">");
		}catch (IOException e) {
			throw new IllegalArgumentException("IOException , <sampleID:" + sampleID +">");
		}catch (NullPointerException e) {
			throw new IllegalArgumentException("can't distinguish the sex of <sampleId:" + sampleID +">");
		}finally{
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if( (e1/(e1+e2)) < 0.1 ) {
			return Sex.Male;
		} else {
			return Sex.Female;
		}
	}
}

package test;

import genome.ManageDB;

import java.io.IOException;


public class TestStore {

	public static void main(String[] args) throws Exception {
		
		// final String consensu_data_dir = "/home/denjo/work/genome/base_data/";
		final String consensu_data_dir = "/home/denjo/Documents/workspace/Consensus2VCF/etc/consensus_data/";
		String input_file_prefix = "10M_";
		
		int chr = 12;
		String runID = "fromConsensus00C";
		String filename;
		
		long t0 = System.nanoTime();
		
		try{
			
			for (int i = 0; i < 1; i++) {
				filename =  consensu_data_dir + input_file_prefix + i ;
				//ManageDB.store(runID, String.valueOf(i) , chr, filename);
				ManageDB.store(runID, String.valueOf(i) , chr, filename + ".bz2");	

			}
			
		}catch (IOException e){
			e.printStackTrace();
		}

		long t1 = System.nanoTime();
		ManageDB.printOneSample(runID, String.valueOf( 0 ) , chr);
		long t2 = System.nanoTime();
		
		printTime(t0,t1,t2);
	}

	
	static void printTime(long... nanos) {
		for(int i = 0; i< nanos.length -1; ++i) {
			System.out.println(  ( nanos[i+1] - nanos[i] ) / (1000* 1000 * 1000 +0.0) + "sec" );
		}
	}
	
}


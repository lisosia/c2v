package test;

import genome.ReferenceReader;

public class testReferenceReader {
	public static void main(String[] args) throws Exception{
		if(args.length!=4){
			System.err.println("Usage: java javafile referenceDBpath chr(string) from(int) to(int)");
			System.exit(1);
		}
		final String refDBPath = args[0];
		String chr = args[1]; //1-22 \ X | Y
		int from = Integer.parseInt(args[2]);
		int to   = Integer.parseInt(args[3]);
		ReferenceReader rr = new ReferenceReader(chr , refDBPath);
		for (int i = from; i < to+1; i++) {
			System.out.println(  "pos:" + i + " ref:" +  rr.read(i) );
		}
	}
}

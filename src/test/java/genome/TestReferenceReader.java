package genome;

import genome.chr.Chr;
import genome.chr.ChrSetFactory;
import genome.format.ReferenceReader;

public class TestReferenceReader {
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: java javafile referenceDBpath chr([1-22]|X|Y) from(int) to(int)");
			System.exit(1);
		}
		final String refDBPath = args[0];
		Chr chr = ChrSetFactory.getHumanChrSet().getChr(args[1]); // 1-22 \ X |
																	// Y
		int from = Integer.parseInt(args[2]);
		int to = Integer.parseInt(args[3]);
		System.err.println("read and print referenec, chr=" + chr + ",from" + from + "to" + to);
		ReferenceReader rr = new ReferenceReader(chr, refDBPath);
		for (int i = from; i < to + 1; i++) {
			// System.out.println( "pos:" + i + " ref:" + rr.read(i) );
			System.out.print(rr.read(i));
			// if(i%50==0){System.out.println("\n");}
		}
	}
}

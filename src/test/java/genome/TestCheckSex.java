package genome;

import genome.CheckSex;

public class TestCheckSex {
	public static void main(String[] args) {
		CheckSex checker = null;
		try {
			checker = new CheckSex(
					"/home/denjo/DOCS/workspace/Consensus2VCF/etc/sex373/sex373a", 0.1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.err.println( checker.getSex("0079") ); 
	}
}

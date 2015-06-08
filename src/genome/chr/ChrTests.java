package genome.chr;

public class ChrTests {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ChrSet chrs = ChrSetFactory.getHumanChrSet();
		for (int i = 1; i <= chrs.getNormalChrSize(); i++) {
			System.out.println( chrs.getChr(i).toString() );
		}
		System.out.println( chrs.getChr("Z").toString() );
	}

}

package genome.chr;

public class ChrTests {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ChrSet chrs = ChrSetFactory.getHumanChrSet();
		
		System.out.println("TEST1");
		for (int i = 1; i <= chrs.getNormalChrSize(); i++) {
			System.out.println( chrs.getChr(i).toString() );
		}
		System.out.println( chrs.getChr("X").toString() );
		
		
		System.out.println("TEST2");
		for (Chr chr : chrs.getNormalChrs()) {
			System.out.println(chr);
		}

	
		System.out.println("TEST3 Male");
		for (Chr chr : chrs.getSexChrs(Sex.Male) ) {
			System.out.println(chr);
		}
		System.out.println("TEST3 Female");
		for (Chr chr : chrs.getSexChrs(Sex.Female) ) {
			System.out.println(chr);
		}
	
	
	}

}

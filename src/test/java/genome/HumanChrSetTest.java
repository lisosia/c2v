package genome;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;
import genome.chr.Sex;

public class HumanChrSetTest {
	
	@Test
	public void testHumanChrSetProps() {
		final ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
		assertNotNull(humanChrSet);

		List<Chr> normalChrs = humanChrSet.getNormalChrs();
		assertEquals(22,normalChrs.size() );
		for (Chr chr: normalChrs ){
//			System.err.println( chr.toString() );
			assertFalse( chr.isSexChr() );
			assertFalse( chr.isSexChr(Sex.Male) );
			assertFalse( chr.isSexChr(Sex.Female) );
		}

		List<Chr> sexChrs = humanChrSet.getSexChrs();
		assertEquals(2,sexChrs.size() );
		assertEquals(23, sexChrs.get(0).getNumForDB());
		assertEquals(24, sexChrs.get(1).getNumForDB());
		for (Chr chr: sexChrs ){
//			System.err.println( chr.toString() );
			assertTrue( chr.isSexChr() );
			if(chr.getStr().equals("X")){
				assertTrue( chr.isSexChr(Sex.Male) );
				assertTrue( chr.isSexChr(Sex.Female) );				
			}else if (chr.getStr().equals("Y")) {
				assertTrue( chr.isSexChr(Sex.Male) );
				assertFalse( chr.isSexChr(Sex.Female) );								
			}
		}
		
		List<Chr> sexChrsMale = humanChrSet.getSexChrs( Sex.Male );
		assertEquals(2,sexChrsMale.size() );
		assertEquals(23, sexChrsMale.get(0).getNumForDB());
		assertEquals(24, sexChrsMale.get(1).getNumForDB());

		for (Chr chr: sexChrsMale ){
			System.err.println( chr.toString() );
			assertTrue( chr.isSexChr() );
			assertTrue( chr.isSexChr(Sex.Male) );
		}
		
		List<Chr> sexChrsFemale = humanChrSet.getSexChrs( Sex.Female);
		assertEquals(1 ,sexChrsFemale.size() );
		assertEquals(23, sexChrsMale.get(0).getNumForDB());
		
		for (Chr chr: sexChrsFemale){
			System.err.println( chr.toString() );
			assertTrue( chr.isSexChr() );
			assertTrue( chr.isSexChr(Sex.Female) );
		}


	}

}

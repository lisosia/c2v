package genome;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;

import genome.chr.Sex;
import genome.format.CheckSex;

public class CheckSexTest {
	
	@Test
	public void testLoad() throws FileNotFoundException, IOException {
		CheckSex checker = null;
		String f = getClass().getResource("/genome/check_sex_input_0").getFile();
		checker = new CheckSex( f, 0.1);

		assertEquals( checker.getSex("0078"), Sex.Female);
		assertEquals( checker.getSex("0079"), Sex.Female);
		assertEquals( checker.getSex("0080"), Sex.Male);
		assertEquals( checker.getSex("0081"), Sex.Male);
	}
}

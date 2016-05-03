package genome.format;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;
import genome.format.ReferenceReader;

public class ReferenceReaderTest {
	
	String path0 = getClass().getResource("/referenceDB/ref.0").getFile();
	ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
	Chr c = humanChrSet.getChr(7);

	@Test
	public void read() throws IOException, SQLException{

		for(Chr c: humanChrSet.getNormalChrs()){
			ReferenceReader ref = new ReferenceReader( c, path0);
			assertEquals( ref.read( 1 ) , 'a');
			assertEquals( ref.read( 10+1 ) , 'a');
			assertEquals( ref.read( 10+2 ) , 'c');
			assertEquals( ref.read( 10+3 ) , 'g');
			assertEquals( ref.read( 10+4 ) , 't');
			assertEquals( ref.read( 90+1 ) , 'a');
		}
	
	}
	
	@Test(expected= IllegalArgumentException.class)
	public void testIllegalCall1() throws IOException, SQLException{
		
		ReferenceReader ref = new ReferenceReader( c, path0);
		ref.read(0);
	}

	@Test(expected= IllegalArgumentException.class)
	public void testIllegalCall2() throws IOException, SQLException{
		
		ReferenceReader ref = new ReferenceReader( c, path0);
		ref.read(5);
		ref.read(5);
	}

	@Test(expected= IllegalArgumentException.class)
	public void testIllegalCall3() throws IOException, SQLException{
		ReferenceReader ref = new ReferenceReader( c, path0);
		ref.read(8);
		ref.read(3);
	}

	@Test(expected= IllegalArgumentException.class)
	public void testIllegalCall4() throws IOException, SQLException{
		ReferenceReader ref = new ReferenceReader( c, path0);
		ref.read(15);
		ref.read(3);
	}

	@Test(expected= IllegalArgumentException.class)
	public void testIllegalCall5() throws IOException, SQLException{
		ReferenceReader ref = new ReferenceReader( c, path0);
		ref.read(0);
	}

	@Test(expected= IllegalArgumentException.class)
	public void testIllegalCall6() throws IOException, SQLException{
		ReferenceReader ref = new ReferenceReader( c, path0);
		ref.read(-1);
	}

}

package test;

import genome.ConsensusReader;
import genome.chr.Chr;
import genome.chr.ChrSet;
import genome.chr.ChrSetFactory;
import genome.chr.Sex;

import java.io.IOException;

public class TestConsensusReader {

	public static void main(String[] args) throws IOException {
		ChrSet humanChrSet = ChrSetFactory.getHumanChrSet();
		Chr chr = humanChrSet.getChr(13);
		ConsensusReader cr = new ConsensusReader(
				"/home/denjo/Documents/workspace/Consensus2VCF/src/etc/input1.consensus", Sex.Male, chr);
		ConsensusReader.ConsensusLineInfo info = new ConsensusReader.ConsensusLineInfo(
				0, 0);
		while (cr.readFilteredLine(info)) {
			System.out.println(info.position);
			System.out.println(info.altsComparedToRef[0]);
			System.out.println(info.altsComparedToRef[1]);
		}
	}

}

package genome;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class ConsensusReader {
	BufferedReader br;
	Matcher matcher = null;
	ConsensusLineInfo lineInfo;

	public ConsensusReader(String filename) throws IOException {
		InputStream is = FileOpen.openCompressedFile(filename);
		this.br = new BufferedReader(new InputStreamReader(is));
	}

	/**
	 * lineInfo = new ConsensusLineInfo(filterQual, filterDP); してから、それを引数として渡す.
	 * 指定した条件に合う行だけをとってきて、 引数lineinfoに格納する
	 * 
	 * @return true. 全て読みきった時はfalse.
	 * @throws IOException
	 */
	public boolean readFilteredLine(ConsensusLineInfo lineInfo)
			throws IOException {
		String line;

		while ((line = br.readLine()) != null && !line.equals("")) {
			if(line.charAt(0)=='#'){continue;} // comment in consensus file
			if (!lineInfo.parseLine(line)) {
				// TODO いまのところ false を返すのは INDEL の時のみ
				continue;
			}

			if (lineInfo.isReliable()) {
				return true;
			} else {
				continue;
			}
		}

		return false;
	}

	public static class ConsensusLineInfo {
		// chr pos (id) ref alt qual (filter==DOT) info(DP=?; rarely,INDEL;)
		// format(pl) BAM[0/0 or ...]num(謎)
		// TODO bam の形式、その意味が　不明
		final static Pattern pattern = Pattern.compile("chr(\\d+)\\t" // CHR
				+ "(\\d+)\\t" // POS
				+ "\\.\\t" // ID
				+ "(\\S+)\\t" // REF
				+ "(\\S+)\\t" // ALT
				+ "(\\S+)\\t" // QUAL
				+ "\\.\\t" // FILTER
				+ "(\\S+)\\t" // INFO
				+ "(\\S+)\\t" // FORMAT
				+ "\\S+"); // BAM

		public int chr; // X,Yの時はそれぞれ -1,0 とする
		public int position;
		public int[] altsComparedToRef;
		public float qual;
		public boolean isIndel;
		public int dp;

		final private double minimumQual;
		final private int minimumDP;

		public ConsensusLineInfo(double minimumQual, int minimumDP) {
			altsComparedToRef = new int[2];
			this.minimumQual = minimumQual;
			this.minimumDP = minimumDP;
		}

		public boolean parseLine(String line) {
			Matcher m = pattern.matcher(line);
			if (!m.matches()) {
				throw new IllegalArgumentException("patternMatch failed. " + 
						"consensus line:\n" + line);
			}
			this.chr = Integer.parseInt(m.group(1));
			this.position = Integer.parseInt(m.group(2));
			String ref = m.group(3);
			String alts = m.group(4);
			this.qual = Float.parseFloat(m.group(5));
			String info = m.group(6);
			// TODO bam(最後の) なにに使うの???
			String bam = m.group(7);
			if (info.contains("INDEL")) {
				return false;
			} // INDEL 即リターン. この行を変えるときは, 後ろでバグらないよう注意

			// set altsComparedToRef
			if (alts.equals(".")) {
				altsComparedToRef[0] = altsComparedToRef[1] = 0;
			} else if (alts.length() == 1) {
				altsComparedToRef[0] = altsComparedToRef[1] = ParseBase
						.returnDiff(ref, alts);
			} else if (alts.length() == 3) {
				altsComparedToRef[0] = ParseBase.returnDiff(ref,
						alts.substring(0, 1));
				altsComparedToRef[1] = ParseBase.returnDiff(ref,
						alts.substring(2, 3));
			} else {
				// NEVER Reach HERE if consensus is correct format
				throw new IllegalArgumentException("patternMatch failed. " + 
						"consensus line:\n" + line);
			}

			// set dp, from INFO
			Pattern infoPatttern = Pattern.compile("DP=(\\d+)\\S+");
			Matcher infoMatcher = infoPatttern.matcher(info);
			if (infoMatcher.matches()) {
				this.dp = Integer.parseInt(infoMatcher.group(1));
			} else {
				// NEVER REACH here
				throw new IllegalArgumentException(
						"expression [ DP=(num) ] not foud in <INFO>\n" +
						"info is following:\n" + info);
			}
			
			return true;
		}

		boolean isReliable() {
			if (!this.isIndel && qual >= this.minimumQual
					&& dp >= this.minimumDP) {
				return true;
			} else {
				return false;
			}
		}

	}

}

final class ParseBase {
	/**
	 * @param ref
	 *            1文字 [ACGT]
	 * @param alt
	 *            1文字 [ACGT]
	 */
	static int returnDiff(String ref, String alt) {
		return (returnNum(alt) - returnNum(ref) + 4) % 4;
	}

	private static int returnNum(String base) {
		switch (base) {
		case "A":
			return 0;
		case "C":
			return 1;
		case "G":
			return 2;
		case "T":
			return 3;
		default:
			break;
		}
		return -1;
	}
}

final class FileOpen {
	static InputStream openCompressedFile(String filename) throws IOException {
		InputStream in = new BufferedInputStream(new FileInputStream(filename));
		if (filename.endsWith(".gz")) {
			return new GZIPInputStream(in);
		} else if (filename.endsWith(".bz2")) {
			return new MultiStreamBZip2InputStream(in);
		} else {
			return in;
		}
	}
}
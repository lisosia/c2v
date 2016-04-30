package genome;

import genome.chr.Chr;
import genome.chr.Sex;

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
	final Sex sex;
	final Chr chr;

	public ConsensusReader(final String filename, final Sex sex, final Chr chr) throws IOException {
		this.sex = sex;
		InputStream is = FileOpen.openCompressedFile(filename);
		this.br = new BufferedReader(new InputStreamReader(is));
		this.chr = chr;
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
				// TODO false を返すのは INDEL or ref=="N" の時, 不正なので読み飛ばす
				continue;
			}
			
			// 男性のXY染色体の時, 非PAR, altsStr.length()==3 && bamが 0/1 のものは信頼できない 
			// misscall となる
			if( (chr.getNumForDB()==23 && this.sex == Sex.Male && lineInfo.altsStr.length()==3 
					&& lineInfo.alts_num == 1 && !isPAR_X(lineInfo.position) ) || 
				(chr.getNumForDB()==23 && this.sex == Sex.Male && lineInfo.altsStr.length()==3 
					&& lineInfo.alts_num == 1 && !isPAR_Y(lineInfo.position))	) 
			{continue;}
			
			if (lineInfo.isReliable()) {
				return true;
			} else {
				continue;
			}
		}

		return false;
	}
	
	/**
	 * X染色体についてのみ
	 * @param pos
	 * @return
	 */
	public static boolean isPAR_X(int pos) {
		if (((pos > 2699520) && (pos < 154931044)) || (pos <60001 ) || (pos > 155270560)) {
			return false;
		}else {
			return true;
		}
	}
		
	public static boolean isPAR_Y(int pos) {
		if(((pos > 2649520) && (pos < 59034050)) || (pos <10001 ) || (pos > 59373566)) {
			return false;
		}else {
			return true;
		}
	}
	
	public static class ConsensusLineInfo {
		// chr pos (id) ref alt qual (filter==DOT) info(DP=?; rarely,INDEL;)
		// format(pl) BAM[0/0 or ...]num(謎)
		// TODO bam の形式、その意味が　不明
		final static Pattern pattern = Pattern.compile(
				"chr(\\S+)\\t" //1: CHR
				+ "(\\d+)\\t" // 2: POS
				+ "\\S+\\t" //       ID
				+ "(\\S+)\\t" // 3: REF
				+ "(\\S+)\\t" // 4: ALT
				+ "(\\S+)\\t" // 5: QUAL
				+ "\\.\\t"    //    FILTER
				+ "(\\S+)\\t" // 6: INFO
				+ "(\\S+)\\t" // 7: FORMAT
				+ "(\\S+)"); //  8: BAM

		public String chr; // X,Yの時はそれぞれ -1,0 とする
		public int position;
		public String altsStr;
		public int[] altsComparedToRef;
		public float qual;
		public boolean isIndel;
		public int dp;
		public int alts_num;
		public String genoType;
		
		final private double minimumQual;
		final private int minimumDP;

		public ConsensusLineInfo(double minimumQual, int minimumDP) {
			altsComparedToRef = new int[2];
			this.minimumQual = minimumQual;
			this.minimumDP = minimumDP;
		}
		
		/**
		 * line をparseしてlineinfoに格納する. 変異が片方だけの時は altsComparedToRef[0]のほうが変異(非0)
		 * @param line
		 * @return 入力が有効な(INDELなどでない)ときはtrue
		 * @throws IllegalArgumentException 入力のフォーマットが不正の時
		 */
		public boolean parseLine(String line) {
			Matcher m = pattern.matcher(line);
			if (!m.matches()) {
				throw new IllegalArgumentException("patternMatch failed. " + 
						"consensus line:\n" + line);
			}
			this.chr = m.group(1);
			this.position = Integer.parseInt(m.group(2));
			String ref = m.group(3);
			if(ref.equals("N")) {return false;}
			this.altsStr = m.group(4);
			this.qual = Float.parseFloat(m.group(5));
			String info = m.group(6);
			this.genoType = m.group(8).split(":")[0];
			
			this.isIndel = false;
			if (info.contains("INDEL")) {
				this.isIndel = true;
				return false;
			} // INDEL あったら即リターン. この行を変えるときは, 後ろでバグらないよう注意

			
			//TODO 1/0 はあるのか
			if(genoType.equals(".")){
				alts_num = 0;
			}else if(genoType.equals("0/1")){
				alts_num = 1;
			}else {
				alts_num = 2;
			}
			
			// set altsComparedToRef
			if (altsStr.equals(".")) { // 変異なし
				altsComparedToRef[0] = altsComparedToRef[1] = 0;
			} else if (altsStr.length() == 1) { // alts == [ACGT] and different from ref(ACGT)
				altsComparedToRef[0] = ParseBase.returnDiff(ref, altsStr);
				if(alts_num==2){
					altsComparedToRef[1] = altsComparedToRef[0];					
				} else {
					altsComparedToRef[1] = 0;										
				}
			} else if (altsStr.length() == 3) {
				altsComparedToRef[0] = ParseBase.returnDiff(ref,altsStr.substring(0, 1));
				altsComparedToRef[1] = ParseBase.returnDiff(ref,altsStr.substring(2, 3));
				if(alts_num==1){ //TODO genotypeと矛盾
					
				}
				alts_num = 2; //TODO 暫定処理
			} else {
			    alts_num = 3;
				// NEVER Reach HERE if consensus is correct format
				// --- Can Reach here
			        //throw new IllegalArgumentException("patternMatch failed. " +  "consensus line:\n" + line);
			}

			
			// set dp, from INFO
			Pattern infoPatttern = Pattern.compile("\\S*DP=(\\d+)\\S*");
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

		public boolean isReliable() {
			if (!this.isIndel && qual >= this.minimumQual
					&& dp >= this.minimumDP ) {
				return true;
			} else {
				return false;
			}
		}
		
		public String toString() {
		/*	public String chr; // X,Yの時はそれぞれ -1,0 とする
			public int position;
			public String altsStr;
			public int[] altsComparedToRef;
			public float qual;
			public boolean isIndel;
			public int dp;
			public int alts_num;
			public String genoType;
			*/
			StringBuilder sb = new StringBuilder();
			sb.append("chr;" + chr)
				.append(" pos:" + position)
				.append(" altsStr:" + altsStr)
				.append(" altsCompToRef[0,1]:" + altsComparedToRef[0] +","+ altsComparedToRef[1])
				.append(" qual:" + qual)
				.append(" isIndel:" + isIndel)
				.append(" dp:" + dp)
				.append(" alts_num:" + alts_num)
				.append(" genoType:" + genoType)
				.append("\n");
			return sb.toString();				
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
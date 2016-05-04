package genome;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import genome.GenomeDataStoreUtil.PersonalGenomeDataDecompressor;
import genome.chr.Chr;
import genome.chr.Sex;

public class Merge {

	final AppConfig c;
	final ManageDB MDB;

	public Merge(String configFilePath, String checkSexFilePath) throws FileNotFoundException, IOException  {
		this.c = new AppConfig(configFilePath);
		this.MDB = new ManageDB(configFilePath, checkSexFilePath);
	}

	public void printDiffByChr(Chr chr, Map<String, ArrayList<String>> id, PrintStream out, final boolean printNotAlts)
			throws ClassNotFoundException, SQLException, IOException {

		for (int pos : MDB.getExistingPosIndex(chr, id.keySet())) {
			int[] merged = getMergedData(chr, id, pos);
			// String chr_str = (chr==23)? "X" : (chr==24) ? "Y" :
			// String.valueOf(chr);
			new PrintData(chr, c.referenceDBPath.getAbsolutePath(), printNotAlts).printMergedData(merged, pos, out);
		}
	}

	/**
	 * @param id
	 *            Map<runID, ArrayList of sampleIDs> mergeするData特定用
	 */
	int[] getMergedData(Chr chr, Map<String, ArrayList<String>> id, int pos_index)
			throws IOException, SQLException, ClassNotFoundException {
		int[] ret = new int[MergeArrayFormat.SIZE_PER_BASE * (c.DATA_SPLIT_UNIT + 1)];
		for (String runID : id.keySet()) {
			int[] mergedByRunID = getMergedDataByRunID(chr, runID, id.get(runID), pos_index);
			assert (ret.length == mergedByRunID.length);
			for (int i = 0; i < mergedByRunID.length; i++) {
				ret[i] += mergedByRunID[i];
			}
			mergedByRunID = null; // GCを期待
		}
		return ret;
	}

	int[] getMergedDataByRunID(Chr chr, String runID, List<String> sampleIDs, int pos_index)
			throws SQLException, ClassNotFoundException, IOException {
		if (sampleIDs.size() == 0) {
			throw new IllegalArgumentException("sampleIDs[of " + runID + "] 's size == 0");
		}
		if (!MDB.dbExists(runID, chr)) {
			throw new IllegalArgumentException("DB does not exist. runID,chr:" + runID + "," + chr);
		}
		StringBuilder sb = new StringBuilder(
				"select * from " + c.TABLE_NAME + " where pos_index = ? and sample_id in (");

		boolean isFirst = true;
		Connection con = MDB.getConnection(runID, chr);
		// con.setReadOnly(true);

		for (String id : sampleIDs) {
			if (!MDB.checkDataExistance(con, runID, id, chr)) {
				throw new Error("[runID,sampleID,chr]:[" + runID + "," + id + "," + chr
						+ "] does not exist! terminate process.");
			}
			if (isFirst) {
				sb.append("'").append(id).append("'");
			} else {
				sb.append(",").append("'").append(id).append("'");
			}
			isFirst = false;
		}
		sb.append(")");
		final String sql = sb.toString();
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setQueryTimeout(c.SQLITE_TIMEOUT_SEC);
		// ps.setInt(1, chr.getNumForDB() ); REMOVED
		ps.setInt(1, pos_index);
		ResultSet rs = ps.executeQuery();

		// for validation
		HashMap<String, Boolean> gotData = new HashMap<String, Boolean>();
		for (String id : sampleIDs) {
			gotData.put(id, false);
		}

		int[] ret = new int[MergeArrayFormat.SIZE_PER_BASE * (c.DATA_SPLIT_UNIT + 1)]; // AA,AC,...,TT,
																						// _A,_B,_C,_T,
																						// isALT
		int gotIdNum = 0;
		while (rs.next()) {
			gotIdNum += 1;
			final String gotID = rs.getString("sample_id");
			assert gotData.containsKey(gotID);
			gotData.put(gotID, true);

			System.err.println("merging   sample_id:" + gotID + " pos_index:" + String.valueOf(pos_index));
			PersonalGenomeDataDecompressor d = new PersonalGenomeDataDecompressor(rs.getBytes("pos_array"),
					rs.getBytes("base_array"));
			int[] data = new int[3];

			while (d.readNext(data) != -1) {
				final int base1 = data[0];
				final int base2 = data[1];
				if (base2 == 0b1111) { // base2は0b11にならない
					System.err.println(" " + base1 + " " + base2 + " " + data[2]);
					throw new IllegalArgumentException("base is 0b1111. internal error");
				}
				final int posRead = data[2];
				try {
					final int BASE_DX = MergeArrayFormat.SIZE_PER_BASE * (posRead - pos_index);
					if ((base1 != 0 && base1 != 0b1111) || base2 != 0) {
						ret[BASE_DX + MergeArrayFormat.IS_ALT_DX] = 1;// flag
																		// for
																		// !=ref
					}
					if (base1 != 0b1111) {
						ret[BASE_DX + MergeArrayFormat.getSubIndex(base1, base2)] += 1;
						ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] += 2;
					} else {
						ret[BASE_DX + MergeArrayFormat.getSubIndex(base1, base2)] += 1; // same
						ret[BASE_DX + MergeArrayFormat.TOTAL_AN_DX] += 1;
					}

				} catch (ArrayIndexOutOfBoundsException e) {
					System.err.println(
							"pos_index,posread,base1,base2:" + pos_index + "," + posRead + "," + base1 + "," + base2);
					throw new RuntimeException("array index outofBounds", e);
				}
			}

		}

		// Yのときは数が合わなくても、スルーしてしまう
		// !chr.getStr().equals("Y") のとき、大量にMegが出力されるはず、、、どうしようか。
		if (gotData.containsValue(false)) {
			final String preMsg = "missing DB Data ::\n" + "chr:" + chr + ",runID:" + runID + ",position(index):"
					+ pos_index + "\n" + "missing sample_id(s):";
			if (!chr.getStr().equals("Y")) {
				String Msg = "";
				for (Map.Entry<String, Boolean> e : gotData.entrySet()) {
					if (e.getValue() == false) {
						Msg += (e.getKey() + ",");
					}
				}
				System.err.println(preMsg + Msg);
				// throw new IllegalArgumentException("Warning:
				// DBファイル内に,要求されたすべてのサンプルIDが含まれていません");

			} else { // when chrY
				String Msg = "";
				for (Map.Entry<String, Boolean> e : gotData.entrySet()) {
					if (MDB.checkSex.getSex(e.getKey()) == Sex.Female) {
						continue;
					} // Femaleのときは関係なし
					if (e.getValue() == false) {
						Msg += (e.getKey() + ",");
					}
				}
				if (!Msg.equals("")) {
					System.err.println(preMsg + Msg);
				}

			}

		}

		con.close();
		return ret;
	}

}
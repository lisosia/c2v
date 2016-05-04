package genome;

import static genome.util.Utils.error2StackTrace;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import genome.GenomeDataStoreUtil.PersonalGenomeDataCompressor;
import genome.GenomeDataStoreUtil.PersonalID;
import genome.chr.Chr;
import genome.chr.Sex;
import genome.format.ConsensusReader;

public class Store {

	final AppConfig c;
	final ManageDB MDB;
	
	public Store(String configFilePath, String checkSexFilePath) throws Exception{
		this.c = new AppConfig(configFilePath);
		this.MDB = new ManageDB(configFilePath, checkSexFilePath);
	}

	public void store(String runID, String sampleID, final Chr chr, String filename) {
		if (MDB.dbExists(runID, chr)) {
			// System.err.println("database already exists, start storing");
		} else {
			System.err.println("database not exist. start creating db.");
			try {
				MDB.initDB(runID, chr);
			} catch (ClassNotFoundException | SQLException e) {
				throw new RuntimeException("error while initting db", e);
			}
		}
	
		System.err.printf("Storing...[runId:%s,sampleId:%s,chr:%s]", runID, sampleID, chr);
	
		long t0 = System.nanoTime();
		final PersonalID pid = new PersonalID(runID, sampleID);
		try {
			parseAndStoreDB(pid, chr, filename);
		} catch (SQLException | IOException | ClassNotFoundException e) {
			System.err.printf("error while storing runID:%s,sampleID:%s,chr:%s,filename:%s", runID, sampleID, chr,
					filename);
			System.err.printf("printStacktrace:\n%s", error2StackTrace(e));
	
			try {
				MDB.removeDBData(pid, chr);
				System.err.println("Removed the data");
			} catch (SQLException e2) {
				System.err.println(" Failed to removed the data");
				//TODO more detailed info, by onbject<runid,sampleid,chr,filename>
				throw new RuntimeException("store failed ", e2);
			}
			//TODO more detailed info, by onbject<runid,sampleid,chr,filename>
			throw new RuntimeException("store failed, remove the entry <<OBJ>>>", e);
		}
		long t1 = System.nanoTime();
		System.err.println("Store fin, " + (t1 - t0) / (1_000_000_000 + 0.0) + " sec passed. " + runID + ","
				+ sampleID + "," + chr);
	
	}

	private void parseAndStoreDB(PersonalID pid, final Chr chr, String filename)
			throws SQLException, ClassNotFoundException, IOException {
				// TODO primary制約にひっかかってエラーが出たら、それをキャッチしてクライアントに伝える
			
				Connection con = MDB.getConnection(pid.getRunID(), chr);
				
				if (MDB.checkDataExistance(con, pid.getRunID(), pid.getSampleName(), chr)) {
					String msg = String.format("you tried to store already existing data[pid:%s,chr:%s,filename:%s]", pid,chr,filename);
					msg += String.format("\nCheck sqlite data in %s", new File(c.DATA_DIR, pid.getRunID() ) );
					throw new RuntimeException(msg);
				}
			
				int pos_index_forDB = 0;
				PersonalGenomeDataCompressor cmpBuf = new PersonalGenomeDataCompressor(pid, con, chr, pos_index_forDB);
			
				boolean isFirst = true;
			
				// int line_ct_per_spilit = 0;
			
				ConsensusReader.ConsensusLineInfo lineInfo = new ConsensusReader.ConsensusLineInfo(c.minimumQual, c.minimumDP);
				Sex sampleSex = MDB.checkSex.getSex(pid.getSampleName());
				System.err.println(pid.getSampleName() + "is " + sampleSex.name());
				if (chr.getStr().equals("Y") && sampleSex == Sex.Female) {
					cmpBuf.StoreDB(c.TABLE_NAME);
					System.err.println("stored empty column bacause of <chrY,female>: " + pid.getSampleName());
					con.close();
					return;
				} // DO nothing
				ConsensusReader consensusReader = new ConsensusReader(filename, sampleSex, chr);
			
				int store_counter1 = 0;
				int store_counter2 = 0;
				while (true) {
					if (consensusReader.readFilteredLine(lineInfo) == false) { // 読み込みここまで
						// final STORE
						cmpBuf.StoreDB(c.TABLE_NAME);
						System.err.println("store finished");
						break;
					}
					// System.out.print(lineInfo);
			
					if (!lineInfo.chr.equals(chr.getStr())) {
						throw new IllegalArgumentException("与えられた chr: " + chr.getStr() + " と 入力<filename:" + filename
								+ " の中身> が一致しません\n" + "consensusfile:chr:" + lineInfo.chr);
					}
			
					if (isFirst) {
						isFirst = false;
					}
			
					/*
					 * data spilit (n+1) * DATA_SPLIT_UNIT < (parseしたposition)
					 * のときは、StoreDB そののちにWRITEしている。 よって n*DATA_SPLIT_UNIT < pos =<
					 * (n+1)*DATA_SPLIT_UNIT が分割領域 具体的には
					 * 1-10000000,10000001-20000000,... (positionは1から始まることに注意)
					 */
					if (lineInfo.position > pos_index_forDB + c.DATA_SPLIT_UNIT) {
						// line_ct_per_spilit = 0;
						// STORING
						cmpBuf.StoreDB(c.TABLE_NAME);
						// after store, should reset buffers, and update pos_index
						isFirst = true;
						// 一気に DATA_SPLIT_UNIT以上
						// consensusfileのpositionが"歯抜け"の時もありうるのでこのような処理
						// もし歯抜けときは、空の（＝ compBuf.write をしていない） record を書き込む（resetBuffer
						// -> Storeの流れ）
						boolean isFirstLoop = true;
						while (lineInfo.position > pos_index_forDB + c.DATA_SPLIT_UNIT) {
							if (!isFirstLoop) {
								cmpBuf.StoreDB(c.TABLE_NAME);
							}
							pos_index_forDB += c.DATA_SPLIT_UNIT;
							cmpBuf.resetBuffer(pos_index_forDB);
							isFirstLoop = false;
						}
					}
			
					// line_ct_per_spilit++;
					// WRITE
					// TODO
					if (lineInfo.altsStr.length() != 1 && lineInfo.altsStr.length() != 3) {
						continue;
					}
					if (!chr.isSexChr() || sampleSex == Sex.Female
							|| chr.getStr().equals("X") && ConsensusReader.isPAR_X(lineInfo.position)
					// ACGT １つだが、Yの文も含めて２つ分数える
					) {
						cmpBuf.writeData(lineInfo.position, lineInfo.altsComparedToRef[0], lineInfo.altsComparedToRef[1]);
						store_counter2 += 1;
						/*
						 * System.out.println(" "+(!chr.isSexChr() ) +" "+ (sampleSex ==
						 * Sex.Female) +" "+ (chr.getStr().equals("X") &&
						 * ConsensusReader.isPAR_X(lineInfo.position) ) );
						 */
					} else if (!(chr.getStr().equals("Y") && ConsensusReader.isPAR_Y(lineInfo.position)) && // つまり男性,XY,非PAR
							!lineInfo.genoType.equals("0/1")) { // 0/1のときはmisscall,
																// readFilteredLineしてるのでこの行入らないけど一応
						// 0b1111 はMerge時に無視される
						cmpBuf.writeData(lineInfo.position, 0b1111, lineInfo.altsComparedToRef[0]);
						store_counter1 += 1;
					} else { // 染色体Yで(Maleで)PARのときは何もしない.
						// System.out.println("Do nothing");
					}
			
				}
				System.err.println("stored column[1,2]:" + store_counter1 + " " + store_counter2);
				con.close();
			
			}

	@SuppressWarnings("unused")
	private void storeDB(PreparedStatement ps, String sample_id, int chr, int pos_index, byte[] pos_array, byte[] base_array)
			throws SQLException {
				ps.setQueryTimeout(c.SQLITE_TIMEOUT_SEC);
				// ps.setInt(1, chr);
				ps.setInt(1, pos_index);
				ps.setString(2, sample_id);
				ps.setBytes(3, pos_array);
				ps.setBytes(4, base_array);
				ps.executeUpdate();
			}

}
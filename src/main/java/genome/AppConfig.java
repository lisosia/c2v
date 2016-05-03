package genome;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

//TODO .propery file is better
public class AppConfig {

	final public int SQLITE_TIMEOUT_SEC = 100 * 60;
	final public String TABLE_NAME = "base_data";

	final public String DATA_DIR;
	final public int DATA_SPLIT_UNIT; //100k * 100 is recommended
	final public double minimumQual;
	final public int minimumDP;
	final public double checkSexRatio;
	final public File referenceDBPath;

	public AppConfig(String filePath) throws FileNotFoundException, IOException {

		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			String data_dir_tmp = br.readLine();
			DATA_DIR = data_dir_tmp.endsWith("/") ? data_dir_tmp : data_dir_tmp + "/";
			DATA_SPLIT_UNIT = Integer.parseInt(br.readLine());
			minimumQual = Double.parseDouble(br.readLine());
			minimumDP = Integer.parseInt(br.readLine());
			referenceDBPath = new File(br.readLine());
			checkSexRatio = Double.parseDouble(br.readLine());
		}
		validation();
	}

	private void validation() {
		if (checkSexRatio < 0 || checkSexRatio > 1) {
			throw new RuntimeException("invalid checkSexRatio" + checkSexRatio);
		}

		if (!referenceDBPath.isFile()) {
			throw new RuntimeException("is not file:" + referenceDBPath);
		}
	}
}

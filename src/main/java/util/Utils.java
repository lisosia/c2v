package util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Utils {
	public static String error2StackTrace(Throwable e){
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		pw.flush();
		pw.close();
		return sw.toString();
	}
}

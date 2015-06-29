package ucsc.hadoop.util;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationUtil {
	
	public static void dumpConfigurations(Configuration conf, PrintStream ps) {
		Map<String,String> sortedConfigMap = new TreeMap<String,String>();
		
		for (Map.Entry<String,String> entry : conf) {
			sortedConfigMap.put(entry.getKey(), entry.getValue());
		}
		ps.println("***************** configurations ***************");
		for (Map.Entry<String,String> entry : sortedConfigMap.entrySet()) {
			ps.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		ps.println("***************** configurations ***************");
	}
}

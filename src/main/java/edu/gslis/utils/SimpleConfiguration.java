package edu.gslis.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Provides access to a configuration file with lines of the form <code>parameter: value</code>
 * 
 * @author garrick
 *
 */
public class SimpleConfiguration implements Configuration {
	
	public static final String CLUSTER_MODE = "cluster-mode";
	public static final String CLUSTER_OUTPUT = "cluster-output";
	public static final String DATA = "data-dir";
	public static final String INDEX = "index";
	public static final String QUERIES = "queries";
	public static final String REDUCERS = "reducer-count";
	public static final String RESULTS = "results";
	public static final String STOPLIST = "stoplist";
	
	private Map<String, String> config;
	
	public SimpleConfiguration() {
		this.config = new HashMap<String, String>();
	}
	
	public void read(String file) {
		try {
			Scanner scanner = new Scanner(new File(file));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String key = line.split(":")[0].trim();
				String value = line.substring(line.indexOf(key)+key.length()+1).trim();
				
				this.config.put(key, value);
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			System.err.println("Configuration file not found.");
			System.exit(-1);
		}
	}

	public String get(String key) {
		return this.config.keySet().contains(key) ? this.config.get(key) : null;
	}

	public void set(String key, String value) {
		this.config.put(key, value);
	}

}

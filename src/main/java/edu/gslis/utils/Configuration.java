package edu.gslis.utils;

public interface Configuration {
	
	public static String INDEX = "index";
	public static String QUERIES = "queries";
	public static String STOPLIST = "stoplist";
	
	public void read(String file);
	
	public String get(String key);
	
	public void set(String key, String value);

}

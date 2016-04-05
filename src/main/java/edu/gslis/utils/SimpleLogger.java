package edu.gslis.utils;

import java.io.PrintStream;

public class SimpleLogger {
	
	private String currentClass;
	private PrintStream out;
	
	public static final PrintStream SYSTEM_OUT = System.out;
	public static final PrintStream SYSTEM_ERR = System.err;
	
	public static final String DEBUG = "DEBUG";
	public static final String INFO = "INFO";
	public static final String WARN = "WARN";
	public static final String ERROR = "ERROR";
	
	public SimpleLogger(Class<?> currentClass) {
		this(currentClass, SYSTEM_ERR);
	}

	public SimpleLogger(Class<?> currentClass, PrintStream out) {
		setCurrentClass(currentClass);
		setOutputStream(out);
	}
	
	public void setCurrentClass(Class<?> currentClass) {
		this.currentClass = currentClass.getCanonicalName();
	}
	
	public void setOutputStream(PrintStream out) {
		this.out = out;
	}
	
	public void log(String message, String priority) {
		String loggedMessage = "["+priority+"] "+currentClass+" - "+message;
		out.println(loggedMessage);
	}
	
	public void debug(String message) {
		log(message, DEBUG);
	}

	public void info(String message) {
		log(message, INFO);
	}
	
	public void warn(String message) {
		log(message, WARN);
	}
	
	public void error(String message) {
		log(message, ERROR);
	}

}

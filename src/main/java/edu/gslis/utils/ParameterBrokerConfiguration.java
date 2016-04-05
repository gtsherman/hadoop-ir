package edu.gslis.utils;

import edu.gslis.utils.ParameterBroker;

/**
 * Simple adapter to make the ir-tools ParameterBroker class work with the Configuration interface.
 * 
 * @author garrick
 *
 */
public class ParameterBrokerConfiguration implements Configuration {

	ParameterBroker params;
	
	public void read(String file) {
		params = new ParameterBroker(file);
	}

	public String get(String key) {
		return params.getParamValue(key);
	}

	public void set(String key, String value) {
		params.setParam(key, value);
	}

}

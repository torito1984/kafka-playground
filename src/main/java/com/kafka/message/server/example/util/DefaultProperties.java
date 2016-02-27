package com.kafka.message.server.example.util;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by david on 2/27/16.
 */
public class DefaultProperties {

	private static Properties prop = new Properties();

	static {
		try {
			prop.load(DefaultProperties.class.getResourceAsStream("kafka-message-server-example-properties.prop"));
		} catch (IOException ex) {
			ex.printStackTrace();
		} 
	}
	
	/**
	 * Gets the property value.
	 *
	 * @param propertyKey the property key
	 * @return the property value
	 */
	public static String getPropertyValue(String propertyKey){
		return prop.getProperty(propertyKey);
	}
}

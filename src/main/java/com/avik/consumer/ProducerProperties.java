package com.avik.consumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProducerProperties {
	String result = "";

	public Map<String, String> getPropValues() throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		try (InputStream input = new FileInputStream("config.properties")) {
			Properties prop = new Properties();
			prop.load(input);
			map.put("BOOTSTRAP-SERVERS", prop.getProperty("bootstrapServers"));
			input.close();
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		}
		return map;
	}
}
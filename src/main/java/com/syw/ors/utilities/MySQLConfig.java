package com.syw.ors.utilities;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MySQLConfig {
    private Properties prop;

    public MySQLConfig() {
	prop = new Properties();
	InputStream input = null;
	try {
	    String filename = "mysql.properties";
	    input = MySQLConfig.class.getClassLoader().getResourceAsStream(filename);
	    if (input == null) {
		System.out.println("Sorry, unable to find " + filename);
		return;
	    }
	    prop.load(input);
	} catch (IOException ex) {
	    ex.printStackTrace();
	} finally {
	    if (input != null) {
		try {
		    input.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	}

    }

    public String getProdUserName() {
	return prop.getProperty("ors.mysql.user");
    }

    public String getProdPassword() {
	return prop.getProperty("ors.mysql.password");

    }

    public String getProdHost() {
	return prop.getProperty("ors.mysql.host");

    }

    public int getProdPort() {
	return Integer.parseInt(prop.getProperty("ors.mysql.port"));

    }

    public String getProdSchemaName() {
	return prop.getProperty("ors.mysql.schema.name");

    }

    public String getProdViewName() {
	return prop.getProperty("ors.mysql.view.name");

    }

    public String getQAUserName() {
	return prop.getProperty("ors.qa.mysql.user");
    }

    public String getQAPassword() {
	return prop.getProperty("ors.qa.mysql.password");

    }

    public String getQAHost() {
	return prop.getProperty("ors.qa.mysql.host");

    }

    public int getQAPort() {
	return Integer.parseInt(prop.getProperty("ors.qa.mysql.port"));

    }

    public String getQASchemaName() {
	return prop.getProperty("ors.qa.mysql.schema.name");

    }

    public String getQAViewName() {
	return prop.getProperty("ors.qa.mysql.view.name");

    }
}

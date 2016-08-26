package com.syw.ors.utilities;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class MySQLConnector {
    private static final String PROD_FILENAME = "prod_offers.csv";
    private static final String QA_FILENAME = "qa_offers.csv";
    private MySQLConfig config;
    private static MySQLConnector mySQLConnector;

    private MySQLConnector() {
	config = new MySQLConfig();
    }

    public static MySQLConnector getInstance() {
	if (mySQLConnector == null) {
	    mySQLConnector = new MySQLConnector();
	    return mySQLConnector;
	}
	return mySQLConnector;
    }

    public String generateFile(boolean isProd) throws IOException, SQLException {
	String filename = getFileName(isProd);
	String query = getQuery(isProd);

	MysqlDataSource mysqlDataSource = getMySQLDataSource(isProd);
	Connection conn = mysqlDataSource.getConnection();
	Statement stmt = conn.createStatement();
	// String query1 = "SELECT * FROM ors_kpos_learning_view";
	ResultSet rs = stmt.executeQuery(query);
	int columnCount = rs.getMetaData().getColumnCount();
	String columnNames = "";
	for (int i = 1; i <= columnCount; i++) {
	    columnNames += rs.getMetaData().getColumnName(i) + ",";
	}

	File file = new File(filename);
	// if file doesnt exists, then create it
	if (!file.exists()) {
	    file.createNewFile();
	}

	FileWriter fw = new FileWriter(file.getAbsoluteFile());
	BufferedWriter bw = new BufferedWriter(fw);
	fw.write(columnNames + "\n");
	System.out.println(columnNames);

	while (rs.next()) {
	    String row = "";
	    for (int i = 1; i <= columnCount; i++) {
		row += "\"" + rs.getString(i) + "\",";
	    }
	    System.out.println(row);
	    bw.write(row + "\n");
	}
	bw.close();
	fw.close();

	return filename;
    }

    private MysqlDataSource getMySQLDataSource(boolean isProd) throws SQLException {
	MysqlDataSource dataSource = new MysqlDataSource();
	if (isProd) {
	    dataSource.setUser(config.getProdUserName());
	    dataSource.setPassword(config.getProdPassword());
	    dataSource.setServerName(config.getProdHost());
	    dataSource.setPortNumber(config.getProdPort());
	    dataSource.setDatabaseName(config.getProdSchemaName());
	} else {
	    dataSource.setUser(config.getQAUserName());
	    dataSource.setPassword(config.getQAPassword());
	    dataSource.setServerName(config.getQAHost());
	    dataSource.setPortNumber(config.getQAPort());
	    dataSource.setDatabaseName(config.getQASchemaName());
	}
	return dataSource;
    }

    private String getQuery(boolean isProd) {
	if (isProd) {
	    return "SELECT * FROM " + config.getProdViewName();
	} else {
	    return "SELECT * FROM " + config.getQAViewName();
	    // return "SELECT * FROM ors_kpos_learning_view";
	}
    }

    private String getFileName(boolean isProd) {
	if (isProd) {
	    return PROD_FILENAME;
	} else {
	    return QA_FILENAME;
	}
    }
}

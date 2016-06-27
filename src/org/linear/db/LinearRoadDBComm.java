package org.linear.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author miyuru
 * This class is used to communicate with the HSQLDB used for storing the historical data during
 * the LinearRoad execution
 */
public class LinearRoadDBComm {
	/**
	 * By default we get an in-memory database connection
	 * @return
	 */
	public Connection getDBConnection(){
		try {
			return DriverManager.getConnection("jdbc:hsqldb:mem:linearroad", "SA", "");
		} catch (SQLException e) {
			return null;
		}
	}
	
	public Connection getDBConnection(String host, int port){
		System.out.println("jdbc:hsqldb:hsql://" + host + ":" + port + "/linearroad;ifexists=true");
		try {
			return DriverManager.getConnection("jdbc:hsqldb:hsql://" + host + ":" + port + "/linearroad;ifexists=true", "SA", "");
		} catch (SQLException e) {
			return null;
		}
	}
	
	public void initLinearRoadDB(){
		Connection con = this.getDBConnection();
	
		if(con == null){
			System.out.println("Error : Could not load the database driver...");
			System.exit(-1);
		}
		
		Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS vehicle_info(carid INT NOT NULL, xway INT NOT NULL, p INT NOT NULL, s INT NOT NULL, d INT NOT NULL, b INT NOT NULL, l FLOAT NOT NULL, va INT NOT NULL, n FLOAT NOT NULL);");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS history(carid INT NOT NULL, d INT NOT NULL, x INT NOT NULL, daily_exp INT NOT NULL);");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS lav(segment INT NOT NULL, lav INT NOT NULL, dir INT NOT NULL);");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS toll(carid INT NOT NULL, segment INT NOT NULL, toll INT NOT NULL);");
			con.commit();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void initLinearRoadDB(String host, int port){
		Connection con = this.getDBConnection(host, port);
	
		if(con == null){
			System.out.println("Error : Could not load the database driver...");
			System.exit(-1);
		}
		
		Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.executeUpdate("TRUNCATE TABLE toll;");
			stmt.executeUpdate("TRUNCATE TABLE lav;");
			con.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		try {
			stmt = con.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS vehicle_info(carid INT NOT NULL, xway INT NOT NULL, p INT NOT NULL, s INT NOT NULL, d INT NOT NULL, b INT NOT NULL, l FLOAT NOT NULL, va INT NOT NULL, n FLOAT NOT NULL);");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS history(carid INT NOT NULL, d INT NOT NULL, x INT NOT NULL, daily_exp INT NOT NULL);");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS lav(segment INT NOT NULL, lav INT NOT NULL, dir INT NOT NULL);");
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS toll(carid INT NOT NULL, segment INT NOT NULL, toll INT NOT NULL);");
			con.commit();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method truncates the Linear Road database
	 * @param host
	 * @param dbport
	 */
	public void truncate(String host, int dbport) {
		Connection con = this.getDBConnection(host, dbport);
		
		if(con == null){
			System.out.println("Error : Could not load the database driver...");
			System.exit(-1);
		}
		
		Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.executeUpdate("TRUNCATE TABLE history;");
			stmt.executeUpdate("TRUNCATE TABLE toll;");
			stmt.executeUpdate("TRUNCATE TABLE lav;");
			con.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

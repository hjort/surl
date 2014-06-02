package br.gov.serpro.cetec.surl.persistence.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import br.gov.serpro.cetec.surl.persistence.UrlDAO;

/**
 * @see http://jdbc.postgresql.org/documentation/80/connect.html
 */
public class PostgreSQLUrlDAO implements UrlDAO {

	private Connection conn;
	
	/*
	CREATE USER surl PASSWORD 'surl';
	CREATE DATABASE surl OWNER surl;
	*/

	/*
	DROP TABLE IF EXISTS urls;
	CREATE TABLE urls (
		hash varchar NOT NULL,
		url varchar NOT NULL,
		clicks int NOT NULL DEFAULT 0
	);
	ALTER TABLE urls OWNER TO surl;
	ALTER TABLE urls ADD PRIMARY KEY (hash);
	CREATE INDEX urls_idx ON urls (url);
	
	CREATE TABLE logs (
		id serial NOT NULL PRIMARY KEY,
		time timestamp NOT NULL,
		hash varchar NOT NULL,
		referrer varchar,
		user_agent varchar,
		ip_address varchar,
		country_code char(2)
	);
	ALTER TABLE logs OWNER TO surl;
	CREATE INDEX logs_idx ON logs (hash);
	*/
	
	public PostgreSQLUrlDAO() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		final String url = "jdbc:postgresql://localhost/surl?user=surl&password=surl";
		try {
			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String getUrl(String hash) {
		String url = null;
		try {
			PreparedStatement ps = conn.prepareStatement(
					"SELECT url FROM urls WHERE hash = ?");
			ps.setString(1, hash);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				url = rs.getString("url");
			}
			rs.close();
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return url;
	}

	public void incHash(String hash) {
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(
					"UPDATE urls SET clicks = clicks + 1 " +
					"WHERE hash = ?");
			ps.setString(1, hash);
			ps.executeUpdate();
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void saveUrl(String hash, String url) {
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(
					"INSERT INTO urls (hash, url) " +
					"VALUES (?, ?)");
			ps.setString(1, hash);
			ps.setString(2, url);
			ps.executeUpdate();
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void recriarDados() {
		
		// recriar estruturas
		Statement st;
		try {
			st = conn.createStatement();
			
			// urls
			st.executeUpdate("DROP TABLE IF EXISTS urls");
			st.executeUpdate("CREATE TABLE urls (" +
				"	hash varchar NOT NULL," +
				"	url varchar NOT NULL," +
				"	clicks int NOT NULL DEFAULT 0" +
				")");
			st.executeUpdate("ALTER TABLE urls OWNER TO surl");
			
			// logs
			st.executeUpdate("DROP TABLE IF EXISTS logs");
			st.executeUpdate("CREATE TABLE logs (" +
				"	id serial NOT NULL PRIMARY KEY," +
				"	time timestamp NOT NULL," +
				"	hash varchar NOT NULL," +
				"	referrer varchar," +
				"	user_agent varchar," +
				"	ip_address varchar," +
				"	country_code char(2)" +
				")");
			st.executeUpdate("ALTER TABLE logs OWNER TO surl");
			
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// popular coleções
		String url, hash;
		for (int i = 1; i <= RELATION_COUNT; i++) {
			hash = String.valueOf(i);
			url = "https://www.google.com.br/#q=".concat(hash); 
			saveUrl(hash, url);
		}
		
		// criar índices
		try {
			st = conn.createStatement();
			st.executeUpdate("ALTER TABLE urls ADD PRIMARY KEY (hash)");
			st.executeUpdate("CREATE INDEX urls_idx ON urls (url)");
			st.executeUpdate("CREATE INDEX logs_idx ON logs (hash)");
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void finalizar() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}

package br.gov.serpro.cetec.surl.persistence.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import br.gov.serpro.cetec.surl.persistence.LogDAO;

public class PostgreSQLLogDAO implements LogDAO {

	private Connection conn;

	public PostgreSQLLogDAO() {
		final String url = "jdbc:postgresql://localhost/surl?user=surl&password=surl";
		try {
			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void saveLog(String hash, String referrer, String agent, String ip) {
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(
					"INSERT INTO logs (" +
					"  time, hash, referrer, user_agent, ip_address, country_code) " +
					"VALUES (now(), ?, ?, ?, ?, ?)");
			ps.setString(1, hash);
			ps.setString(2, referrer);
			ps.setString(3, agent);
			ps.setString(4, ip);
			ps.setString(5, "BR"); // TODO: geolocalização
			ps.executeUpdate();
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}

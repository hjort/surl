package br.gov.serpro.cetec.surl.persistence.impl;

import br.gov.serpro.cetec.surl.persistence.LogDAO;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CassandraLogDAO implements LogDAO {

	private Cluster cluster;
	private Session session;
	
	public CassandraLogDAO() {
		cluster = Cluster.builder().addContactPoint("localhost").build();
		session = cluster.connect();
	}

	public void saveLog(String hash, String referrer, String agent, String ip) {
		final PreparedStatement ps = session.prepare(
				"INSERT INTO surl.logs (" +
				"  id, time, hash, referrer, user_agent, ip_address, country_code)" +
				"VALUES (now(), dateof(now()), ?, ?, ?, ?, ?)");
		final BoundStatement bs = new BoundStatement(ps);
		session.execute(bs.bind(hash, referrer, agent, ip, "BR"));
	}

}

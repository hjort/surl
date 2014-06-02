package br.gov.serpro.cetec.surl.persistence.impl;

import br.gov.serpro.cetec.surl.persistence.UrlDAO;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @see http://www.datastax.com/documentation/developer/java-driver/1.0/java-driver/quick_start/qsSimpleClientAddSession_t.html
 * @see http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/cql_data_types_c.html
 * @see http://stackoverflow.com/questions/23145817/update-a-cassandra-integer-column-using-cql
 * @see http://stackoverflow.com/questions/17945341/how-to-auto-generate-uuid-in-cassandra-cql-3-command-line
 * @see http://stackoverflow.com/questions/19623432/how-to-get-current-timestamp-with-cql-while-using-command-line
 */
public class CassandraUrlDAO implements UrlDAO {

	private Cluster cluster;
	private Session session;
	
	/*
	DROP KEYSPACE surl;
	CREATE KEYSPACE surl
	WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
	USE surl;
	*/

	/*
	DROP TABLE urls;
	CREATE TABLE urls (
	  hash text PRIMARY KEY,
	  url text
	);
	CREATE INDEX ON urls (url);
	
	DROP TABLE urls_counters;
	CREATE TABLE urls_counters (
	  hash text PRIMARY KEY,
	  clicks counter
	);
	
	DROP TABLE logs;
	CREATE TABLE logs (
	  id timeuuid PRIMARY KEY,
	  time timestamp,
	  hash text,
	  referrer text,
	  user_agent text,
	  ip_address text,
	  country_code text
	);
	CREATE INDEX ON surl.logs (hash);
	*/

	public CassandraUrlDAO() {
		cluster = Cluster.builder().addContactPoint("localhost").build();
		final Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public String getUrl(String hash) {
		String url = null;
		final ResultSet rs = session.execute(
				"SELECT url FROM surl.urls WHERE hash = '" + hash + "'");
		final Row row = rs.one();
		if (row != null) {
			url = row.getString("url");
		}
		return url;
	}

	public void incHash(String hash) {
		final PreparedStatement ps = session.prepare(
			"UPDATE surl.urls_counters SET clicks = clicks + 1 " +
			"WHERE hash = ?");
		final BoundStatement bs = new BoundStatement(ps);
		session.execute(bs.bind(hash));
	}

	public void saveUrl(String hash, String url) {
		final PreparedStatement ps = session.prepare(
			"INSERT INTO surl.urls (hash, url) " +
			"VALUES (?, ?)");
		final BoundStatement bs = new BoundStatement(ps);
		session.execute(bs.bind(hash, url));
	}

	public void recriarDados() {
		
		// recriar estruturas
		session.execute("DROP KEYSPACE " + DATABASE_NAME);
		session.execute("CREATE KEYSPACE " + DATABASE_NAME + " " +
			"WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};");
		
		// urls
		session.execute("CREATE TABLE surl.urls (" +
			"  hash text PRIMARY KEY," +
			"  url text" +
			");");
		session.execute("CREATE TABLE surl.urls_counters (" +
			"  hash text PRIMARY KEY," +
			"  clicks counter" +
			");");
		
		// logs
		session.execute("CREATE TABLE surl.logs (" +
			"	id timeuuid PRIMARY KEY," +
			"	time timestamp," +
			"	hash text," +
			"	referrer text," +
			"	user_agent text," +
			"	ip_address text," +
			"	country_code text" +
			");");
		
		// popular coleções
		String url, hash;
		for (int i = 1; i <= RELATION_COUNT; i++) {
			hash = String.valueOf(i);
			url = "https://www.google.com.br/#q=".concat(hash); 
			saveUrl(hash, url);
		}

		// criar índices
		session.execute("CREATE INDEX ON surl.urls (url)");
		session.execute("CREATE INDEX ON surl.logs (hash)");
	}

	public void finalizar() {
		session.shutdown();
		cluster.shutdown();
	}

}

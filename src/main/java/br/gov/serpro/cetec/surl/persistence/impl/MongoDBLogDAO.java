package br.gov.serpro.cetec.surl.persistence.impl;

import java.net.UnknownHostException;
import java.util.Date;

import br.gov.serpro.cetec.surl.persistence.LogDAO;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoDBLogDAO implements LogDAO {

	private MongoClient client;
	private DB db;
	private DBCollection coll;
	
	public MongoDBLogDAO() {
		try {
			client = new MongoClient();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		db = client.getDB(DATABASE_NAME);
		coll = db.getCollection(RELATION_NAME);
	}

	public void saveLog(String hash, String referrer, String agent, String ip) {
		final BasicDBObject doc = new BasicDBObject("hash", hash)
			.append("time", new Date())
			.append("referrer", referrer)
			.append("user_agent", agent)
			.append("ip_address", ip)
			.append("country_code", "BR");
		coll.insert(doc);
	}

}

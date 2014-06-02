package br.gov.serpro.cetec.surl.persistence.impl;

import java.net.UnknownHostException;

import br.gov.serpro.cetec.surl.persistence.UrlDAO;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * @see http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-java-driver/
 * @see http://docs.mongodb.org/manual/reference/command/
 * @see http://docs.mongodb.org/manual/reference/bson-types/
 * @see http://docs.mongodb.org/ecosystem/drivers/java/
 */
public class MongoDBUrlDAO implements UrlDAO {

	private MongoClient client;
	private DB db;
	private DBCollection coll;
	
	public MongoDBUrlDAO() {
		try {
			client = new MongoClient();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		db = client.getDB(DATABASE_NAME);
		coll = db.getCollection(RELATION_NAME);
	}

	public String getUrl(String hash) {
		final BasicDBObject query = new BasicDBObject("hash", hash);
		final DBCursor cursor = coll.find(query, new BasicDBObject("url", 1));
		String url = null;
		try {
			if (cursor.hasNext()) {
				DBObject doc = cursor.next();
				url = (String) doc.get("url");
			}
		} finally {
			cursor.close();
		}
		return url;
	}

	public void incHash(String hash) {
		coll.update(new BasicDBObject("hash", hash),
				new BasicDBObject("$inc",
				new BasicDBObject("clicks", 1)));
	}

	public void saveUrl(String hash, String url) {
		final BasicDBObject doc = new BasicDBObject("hash", hash)
			.append("url", url)
			.append("clicks", 0);
		coll.insert(doc);
	}

	public void recriarDados() {
		
		// recriar estruturas
		client.dropDatabase(DATABASE_NAME);
		
		// urls
		db.createCollection("urls", new BasicDBObject("size", 10485760));
		coll = db.getCollection("urls");

		// logs
		db.createCollection("logs", new BasicDBObject("size", 10485760));
		DBCollection col2 = db.getCollection("logs");

		// popular coleções
		String url, hash;
		for (int i = 1; i <= RELATION_COUNT; i++) {
			hash = String.valueOf(i);
			url = "https://www.google.com.br/#q=".concat(hash); 
			saveUrl(hash, url);
		}
		
		// criar índices
		coll.createIndex(new BasicDBObject("hash", 1));
		coll.createIndex(new BasicDBObject("url", 1));
		col2.createIndex(new BasicDBObject("hash", 1));
	}

	public void finalizar() {
		client.close();
	}

}

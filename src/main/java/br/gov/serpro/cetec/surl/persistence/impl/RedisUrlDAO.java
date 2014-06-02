package br.gov.serpro.cetec.surl.persistence.impl;

import java.util.HashMap;
import java.util.Map;

import br.gov.serpro.cetec.surl.persistence.UrlDAO;
import redis.clients.jedis.Jedis;

/**
 * @see https://github.com/xetorthio/jedis
 * @see http://redis.io/commands
 */
public class RedisUrlDAO implements UrlDAO {

	private Jedis jedis;
	
	public RedisUrlDAO() {
	}

	private Jedis getJedis() {
		if (jedis == null || !jedis.isConnected()) {
			jedis = new Jedis("localhost");
		}
		return jedis;
	}
	
	public String getUrl(String hash) {
		/*
		HGET url:keYW0rD url
		*/
		final String key = RELATION_NAME + ":" + hash;
		final String url = getJedis().hget(key, "url");
		return url;
	}

	public void incHash(String hash)
	{
		/*
		HINCRBY url:KeyworD clicks 1
		HGET url:KeyworD clicks
		*/
		final String key = RELATION_NAME + ":" + hash;
		getJedis().hincrBy(key, "clicks", 1);
	}

	public void saveUrl(String hash, String url) {
		/*
		HMSET url:keYW0rD url "http://redis.io/x/" clicks 0
		HGETALL url:keYW0rD
		*/
		final String key = RELATION_NAME + ":" + hash;
		final Map<String, String> map = new HashMap<String, String>();
		map.put("url", url);
		map.put("clicks", "0");
		getJedis().hmset(key, map);
	}

	public void recriarDados() {

		// recriar estruturas
		/*
		KEYS url:*
		DEL url:keYW0rD
		 */
		jedis = getJedis();
		for (String key : jedis.keys(RELATION_NAME + ":*")) {
			jedis.del(key);
		}
		
		// popular coleções
		String url, hash;
		for (int i = 1; i <= RELATION_COUNT; i++) {
			hash = String.valueOf(i);
			url = "https://www.google.com.br/#q=".concat(hash); 
			saveUrl(hash, url);
		}
	}

	public void finalizar() {
		getJedis().close();
	}

}

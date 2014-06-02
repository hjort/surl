package br.gov.serpro.cetec.surl.persistence.impl;

import redis.clients.jedis.Jedis;
import br.gov.serpro.cetec.surl.persistence.LogDAO;

public class RedisLogDAO implements LogDAO {

	private Jedis jedis;
	
	public RedisLogDAO() {
	}

	private Jedis getJedis() {
		if (jedis == null || !jedis.isConnected()) {
			jedis = new Jedis("localhost");
		}
		return jedis;
	}
	
	public void saveLog(String hash, String referrer, String agent, String ip) {
		final String key = RELATION_NAME + ":" + hash;
		final String value = "{hash:'" + hash + "', " +
				"referrer:'" + referrer + "', " +
				"agent:'" + agent + "', " +
				"ip:'" + ip + "'}";
		getJedis().rpush(key, value);
	}

}

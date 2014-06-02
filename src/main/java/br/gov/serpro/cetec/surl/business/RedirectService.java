package br.gov.serpro.cetec.surl.business;

import br.gov.serpro.cetec.surl.persistence.LogDAO;
import br.gov.serpro.cetec.surl.persistence.UrlDAO;
import br.gov.serpro.cetec.surl.persistence.impl.RedisLogDAO;
import br.gov.serpro.cetec.surl.persistence.impl.RedisUrlDAO;

public class RedirectService {

	private UrlDAO urlDAO;
	private LogDAO logDAO;

	public RedirectService() {
		
		// FIXME: parametrizar de alguma forma!
		
//		urlDAO = new PostgreSQLUrlDAO();
//		logDAO = new PostgreSQLLogDAO();
		
//		urlDAO = new CassandraUrlDAO();
//		logDAO = new CassandraLogDAO();
		
//		urlDAO = new MongoDBUrlDAO();
//		logDAO = new MongoDBLogDAO();
		
		urlDAO = new RedisUrlDAO();
		logDAO = new RedisLogDAO();
	}

	/**
	 * @param hash
	 * @param referrer
	 * @param userAgent
	 * @param ipAddress
	 * @return
	 */
	public String record(String hash,
			String referrer, String userAgent, String ipAddress) {
		
		final String site = urlDAO.getUrl(hash);
		
		urlDAO.incHash(hash);
		logDAO.saveLog(hash, referrer != null ? referrer : "", userAgent, ipAddress);
		
		return site;
	}

}

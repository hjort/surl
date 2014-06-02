package br.gov.serpro.cetec.surl.persistence;

public interface LogDAO {
	
	String DATABASE_NAME = "surl";
	String RELATION_NAME = "logs";

	public void saveLog(String hash, String referrer, String agent, String ip);

}

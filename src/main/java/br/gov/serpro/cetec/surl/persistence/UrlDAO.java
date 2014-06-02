package br.gov.serpro.cetec.surl.persistence;

public interface UrlDAO {

	String DATABASE_NAME = "surl";
	String RELATION_NAME = "urls";
	int RELATION_COUNT = 100000;

	public String getUrl(String hash);
	public void incHash(String hash);

	public void saveUrl(String hash, String url);

	public void recriarDados();
	public void finalizar();

}

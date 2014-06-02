package br.gov.serpro.cetec.surl.build;

import java.util.ArrayList;
import java.util.List;

import br.gov.serpro.cetec.surl.persistence.UrlDAO;
import br.gov.serpro.cetec.surl.persistence.impl.CassandraUrlDAO;
import br.gov.serpro.cetec.surl.persistence.impl.MongoDBUrlDAO;
import br.gov.serpro.cetec.surl.persistence.impl.PostgreSQLUrlDAO;
import br.gov.serpro.cetec.surl.persistence.impl.RedisUrlDAO;

public class BuildData {

	@SuppressWarnings("unused")
	private UrlDAO dao;
	
	public static void main(String[] args) {
		BuildData app = new BuildData();
		
//		app.iniciarPostgreSQL();
//		app.iniciarCassandra();
//		app.iniciarMongoDB();
		app.iniciarRedis();
		app.dao.recriarDados();
		app.dao.finalizar();
		
//		app.recriarTodos();
	}

	private void recriarTodos() {
		final List<UrlDAO> daos = new ArrayList<UrlDAO>(4);
		daos.add(new PostgreSQLUrlDAO());
		daos.add(new CassandraUrlDAO());
		daos.add(new MongoDBUrlDAO());
		daos.add(new RedisUrlDAO());
		for (UrlDAO dao : daos) {
			System.out.println(dao.toString());
			dao.recriarDados();
			dao.finalizar();
		}
	}

	@SuppressWarnings("unused")
	private void iniciarPostgreSQL() {
		dao = new PostgreSQLUrlDAO();
	}

	@SuppressWarnings("unused")
	private void iniciarCassandra() {
		dao = new CassandraUrlDAO();
	}

	@SuppressWarnings("unused")
	private void iniciarMongoDB() {
		dao = new MongoDBUrlDAO();
	}

	@SuppressWarnings("unused")
	private void iniciarRedis() {
		dao = new RedisUrlDAO();
	}

}

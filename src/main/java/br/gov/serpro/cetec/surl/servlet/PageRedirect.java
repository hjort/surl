package br.gov.serpro.cetec.surl.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.gov.serpro.cetec.surl.business.RedirectService;
import br.gov.serpro.cetec.surl.persistence.UrlDAO;
import br.gov.serpro.cetec.surl.persistence.impl.CassandraUrlDAO;
import br.gov.serpro.cetec.surl.persistence.impl.PostgreSQLUrlDAO;

/**
 * @see http://www.tutorialspoint.com/servlets/servlets-page-redirect.htm
 */
@SuppressWarnings("unused")
public class PageRedirect extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private RedirectService service;
	
	public PageRedirect() {
		service = new RedirectService();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		// Set response content type
//		response.setContentType("text/html");

		// New location to be redirected
//		final String site = new String("http://www.serpro.gov.br");

		final String uri = request.getRequestURI();
		final String hash = uri.replaceFirst("/[a-z0-9]+/", "");
		
		final String referrer = request.getHeader("referer"); // Yes, with the legendary misspelling
		final String userAgent = request.getHeader("User-Agent");
		final String ipAddress = request.getRemoteAddr();
		
		final String site = service.record(hash, referrer, userAgent, ipAddress);
		
		response.setStatus(HttpServletResponse.SC_MOVED_TEMPORARILY);
		response.setHeader("Location", site);
	}
	
}

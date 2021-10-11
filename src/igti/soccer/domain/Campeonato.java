package igti.soccer.domain;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mileto.util.Security;

import igti.soccer.CargaDadosWeb;
import igti.soccer.datasource.DataSource;
import igti.soccer.datasource.DataSourceEspn;
import igti.soccer.datasource.DataSourceWhoScored;

public class Campeonato implements Comparable, Serializable {

	private String dataSourceURL;
	private DataSource ds;	
	private String sigla;
	private String dataSourcePortal;
	
	static final Log log = LogFactory.getLog(CargaDadosWeb.class);

	/**
	public static Campeonato instance( String url, String dataSource) {
		Campeonato liga = new Campeonato();
		liga.dataSourceURL = url;
	

		if ( dataSource == "ESPN") {
			//liga.ds = new DataSourceEspn();
		}
		return liga;
	}
	**/

	public static Campeonato instance( String linhaToda ) {
		Campeonato liga = new Campeonato();
		StringTokenizer token = new StringTokenizer(linhaToda, "|");
		if (token.hasMoreElements()) {
			String nada = token.nextElement().toString();
			String campeonato = token.nextElement().toString();
			liga.dataSourceURL = token.nextElement().toString();
			


			String dataSource = token.nextElement().toString();
			System.out.println("DATA SOURCE = " + dataSource);
			String charset = token.nextElement().toString();
			liga.sigla = token.nextElement().toString();
			String temporada = token.nextElement().toString();

			//x++;

			if ( dataSource.equals( "ESPN" ) ) {
				liga.ds = new DataSourceEspn( liga.sigla, temporada );
			} else if ( dataSource.equals( "WHOSCORED" ) ) {
				liga.ds = new DataSourceWhoScored( liga.sigla, temporada );
			}
			liga.dataSourcePortal = dataSource;
		}

		log.info(">>>>>>>>>> Definindo o campeonato");
		log.info(">>>>>>>>>> " + liga.dataSourceURL);
		log.info(">>>>>>>>>> " + liga.ds);
		
		
		File hiveDados = CargaDadosWeb.hiveDados.toFile(); 
		if ( !hiveDados.exists()) {
			hiveDados.mkdir();			
		}
		
		Path hiveDadosPlayers = Paths.get( hiveDados.getAbsolutePath()+"/players" );
		Path hiveDadosPaginas = Paths.get( hiveDados.getAbsolutePath()+"/paginas" );
		if ( !hiveDadosPlayers.toFile().exists() ) {
			hiveDadosPlayers.toFile().mkdir();
		}
		if ( !hiveDadosPaginas.toFile().exists() ) {
			hiveDadosPaginas.toFile().mkdir();
		}
		
		return liga;
	}

	public void getAtletas(String nomeClube, URL urlAtletas) throws IOException {
		try { 
			PrintWriter writer = new PrintWriter("/tmp/hivedados/papli.db/coletados/" + "players" + Security.giveMeFuckingName(5)+ ".txt", "UTF-8");
			this.ds.getAtletas ( nomeClube, urlAtletas, writer ).forEach( ( key, value ) -> {
				System.out.println("Jogador: " + key ); 
			});
			writer.close();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}

	public TreeMap<String, URL>getClubes() throws IOException { 
		//PrintWriter writer = new PrintWriter("/tmp/hivedados/papli.db/coletados/" + "players" + Security.giveMeFuckingName(5)+ ".txt", "UTF-8");
		return this.ds.getClubes( new URL(dataSourceURL) );
	}




	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataSourceURL == null) ? 0 : dataSourceURL.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Campeonato other = (Campeonato) obj;
		if (dataSourceURL == null) {
			if (other.dataSourceURL != null)
				return false;
		} else if (!dataSourceURL.equals(other.dataSourceURL))
			return false;
		return true;
	}


	@Override
	public int compareTo(Object o) {
		return Integer.parseInt( Security.giveMeFuckingName(9) );
	}

	public DataSource getDs() {
		return ds;
	}

	public void setDs(DataSource ds) {
		this.ds = ds;
	}

	public String getDataSourceURL() {
		return dataSourceURL;
	}

	public void setDataSourceURL(String dataSourceURL) {
		this.dataSourceURL = dataSourceURL;
	}

	public String getSigla() {
		return sigla;
	}

	public void setSigla(String sigla) {
		this.sigla = sigla;
	}

	public String getDataSourcePortal() {
		return dataSourcePortal;
	}

	public void setDataSourcePortal(String dataSourcePortal) {
		this.dataSourcePortal = dataSourcePortal;
	}

	




}

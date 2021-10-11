package igti.soccer.datasource;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.TreeMap;

public interface DataSource {

	public abstract TreeMap<String, URL> getClubes(URL urlEquipes) throws IOException ;
	
	public abstract TreeMap<String, URL> getAtletas(String nomeClube, URL urlAtletas, PrintWriter writer) throws IOException;
	
	
}

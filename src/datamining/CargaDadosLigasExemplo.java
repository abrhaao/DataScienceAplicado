package datamining;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.jsoup.Jsoup;
//import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
//import org.jsoup.nodes.Element;
//import org.jsoup.select.Elements;
import org.mileto.util.Security;

public class CargaDadosLigasExemplo {

	public static void main(String[] args) {

		getLigas();

	}

	private static void getLigas() {

		TreeSet<String> set = new TreeSet<>();
		set.add("http://www.espn.com.br/futebol/classificacao/_/liga/arg.1");
		set.add("http://www.espn.com.br/futebol/classificacao/_/liga/por.1/season/2017");
		//set.add("https://www.whoscored.com/Regions/11/Tournaments/68/Argentina-Primera-Divisi%C3%B3n");	//proteção pesada com frames
		//set.add("B");

		
		set.stream().forEach( campeonato -> {
			System.out.println( campeonato );
			
			try {
				TreeMap<String, URL> listaClubes = getClubes ( new URL( campeonato) );
				
				listaClubes.forEach((k,v)-> {
					System.out.println ( "------------------------------------------------------" );
					System.out.println("key: " + k + ", value: " + v); 
					System.out.println ( "------------------------------------------------------" );

					
					try {
						PrintWriter writer = new PrintWriter("/tmp/hive/dados/players/" + "players" + Security.giveMeFuckingName(5)+ ".txt", "UTF-8");
						getAtletas ( k, v, writer ).forEach( ( key, value ) -> {
							System.out.println("Jogador: " + key ); 
						});
						writer.close();
						
					} catch ( IOException e) {
						e.printStackTrace();
					} finally { 
						
					}
					

				});

			} catch (IOException e) {
				e.printStackTrace();
			}
		}); 
	}

	private static TreeMap<String, URL> getClubes(URL urlEquipes) throws IOException {

		Process proc = Runtime.getRuntime().exec( "curl " + urlEquipes.toString()  );
		BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));	//Formato varia conforme Liga

		//java.util.Map<String, URL> listaClubes = Collections.synchronizedMap( new HashMap<String, URL>() );

		String line;
		StringBuffer htmlContent = new StringBuffer();
		while ((line = reader.readLine()) != null) {				
			htmlContent.append(line);
		}
		Document docHtml = Jsoup.parse(htmlContent.toString());


		/** TODO desenvolver com interfaces? **/
		if ( urlEquipes.toString().contains("espn") ) {

			Elements eClubes = docHtml.select("tr[class=standings-row]");
			Map<String, URL> listaClubes = new TreeMap<String, URL>();
			for (Element e: eClubes) {
				String[] clube = { e.select("abbr").attr("title"), "http://www.espn.com.br" + e.select("a").get(1).attr("href").replaceAll("/time/_/", "/time/elenco/_/")};
				listaClubes.put( clube[0], new URL(clube[1]));
			}

			//int y = 99;
			return (TreeMap<String, URL>)listaClubes;
		}
		return null;

		/**
			for (Element e: eClubes) {
				e.get(3).select("abbr").attr("title")
			}


			listaClubes.forEach((k,v)-> {
				System.out.println ( "------------------------------------------------------" );
				System.out.println("key: " + k + ", value: " + v); 
				System.out.println ( "------------------------------------------------------" );


				processaSquad ( k, v);




			});


		 **/




	}








	private static TreeMap<String, URL> getAtletas(String nomeClube, URL urlAtletas, PrintWriter writer) throws IOException {


		Process proc = Runtime.getRuntime().exec( "curl " + urlAtletas.toString()  );
		BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));

		String line;
		StringBuffer htmlContent = new StringBuffer();
		while ((line = reader.readLine()) != null) {				
			htmlContent.append(line);
		}
		Document docHtml = Jsoup.parse(htmlContent.toString());


		try { 
			
			Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/my", "abrhaao", "java");
			
			

			/** TODO desenvolver com interfaces? **/
			if ( urlAtletas.toString().contains("espn") ) {

				Map<String, URL> listaJogadores = new TreeMap<String, URL>();
				Elements eJogadores = docHtml.select("tr[class=Table2__tr Table2__tr--sm Table2__even]");			
				for (Element e: eJogadores) {
					//System.out.println( "<funcao>" + e.select("span").get(0).text() + "</funcao>" );
					//System.out.println( "<nome>" + e.select("span").get(2).text() + "</nome>" );

					
					
					StringBuffer sql = new StringBuffer();
					sql.append( "INSERT INTO ds_jogador VALUES ( 44, '" + e.select("span").get(2).text() + "', '" + nomeClube + "', '" + e.select("span").get(0).text() + "', ");
					sql.append( "                                  '" + "caract" + "') ");
					//System.out.println( sql );

					try {
						
						
						writer.println( e.select("span").get(2).text() + "|" + nomeClube + "|" + e.select("span").get(0).text());
						
						
						//Statement stmt = con.createStatement();
						//stmt.executeUpdate(sql.toString());

						//con.commit();
						
						//Thread.sleep(99);
					//} catch (SQLException sqle) { 
						//sqle.printStackTrace();
					} catch (Exception sqle) { 
						sqle.printStackTrace();
					}
				}

				
				//stmt.close();
				con.close();
				return (TreeMap<String, URL>)listaJogadores;
			}
		} catch (Exception sqle) {
			sqle.printStackTrace();
		}

	
		return null;

	}

}

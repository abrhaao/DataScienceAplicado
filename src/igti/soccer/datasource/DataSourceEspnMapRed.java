package igti.soccer.datasource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import scala.Tuple2;


public class DataSourceEspnMapRed implements DataSource {

	private String dsCampeonato;
	private String dsTemporada;


	/**
	 * O contrutor dos Data Sources não precisa ter uma assinatura padrão. Pode ter quantos campos forem necessários para o processamento dos dados
	 * @param dsCampeonato
	 * @param dsTemporada
	 */
	public DataSourceEspnMapRed(String dsCampeonato, String dsTemporada) {
		super();
		this.dsCampeonato = dsCampeonato;
		this.dsTemporada = dsTemporada;
	}

	@Override
	public TreeMap<String, URL> getClubes(URL urlEquipes) throws IOException {
		
		final Log log = LogFactory.getLog(DataSourceEspnMapRed.class);

		Process proc = Runtime.getRuntime().exec( "curl " + urlEquipes.toString()  );
		BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));	//Formato varia conforme Liga

		String line;
		StringBuffer htmlContent = new StringBuffer();
		while ((line = reader.readLine()) != null) {				
			htmlContent.append(line);
		}
		Document docHtml = Jsoup.parse(htmlContent.toString());
		PrintWriter writer = new PrintWriter("/tmp/hive/dados/paginas/" + "campeonato-espn-" + this.dsCampeonato.replace("/","") + ".html", "UTF-8");
		writer.print( docHtml.outerHtml() );
		writer.close();


		if ( urlEquipes.toString().contains("espn") ) {
			
		

			//Elements eClubes = docHtml.select("tr[class=standings-row]");
			Elements eClubes = docHtml.select("tr[class*=Table2__tr Table2__tr--sm Table2__even]");
			Map<String, URL> listaClubes = new TreeMap<String, URL>();
			
			
			
			
			
			
			for (Element e: eClubes) {
				if ( e.select("a").size()  > 0 ) { 
					String linkElenco = e.select("a").get(1).attr("href").replaceAll("/time/_/", "/time/elenco/_/");
					//System.out.println( "Aqui tenho que trocar >> " + linkElenco );

					StringTokenizer str = new StringTokenizer(linkElenco, "/");
					String bufferExclude = new String("#");
					while ( str.hasMoreTokens() ) {
						bufferExclude = str.nextToken();
					}
					System.out.println( "Link clube >> " + linkElenco );


					linkElenco = linkElenco.replaceAll(bufferExclude, this.dsCampeonato + "/temporada/" + this.dsTemporada);

					String[] clube = { e.select("abbr").attr("title"), "http://www.espn.com.br" + linkElenco };
					listaClubes.put( clube[0], new URL(clube[1]));
				}
			}

			//int y = 99;
			return (TreeMap<String, URL>)listaClubes;
		}

		return null;
	}

	@Override
	public TreeMap<String, URL> getAtletas(String nomeClube, URL urlAtletas, PrintWriter writer) throws IOException {
		
		final Log log = LogFactory.getLog(DataSourceEspn.class);
		
		Process proc = Runtime.getRuntime().exec( "curl " + urlAtletas.toString()  );
		BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));

		String line;
		StringBuffer htmlContent = new StringBuffer();
		while ((line = reader.readLine()) != null) {				
			htmlContent.append(line);
		}
		Document docHtml = Jsoup.parse(htmlContent.toString());
		PrintWriter wPagina = new PrintWriter("/tmp/hive/dados/paginas/" + "campeonato-espn-" + this.dsCampeonato.replace("/","") + "-" + nomeClube + ".html", "UTF-8");
		wPagina.print( docHtml.outerHtml() );
		wPagina.close();


		try { 

			//Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/my", "abrhaao", "java");



			/** TODO desenvolver com interfaces? **/
			if ( urlAtletas.toString().contains("espn") ) {

				Map<String, URL> listaJogadores = new TreeMap<String, URL>();
				Elements eJogadores = docHtml.select("tr[class=Table2__tr Table2__tr--sm Table2__even]");			
				for (Element e: eJogadores) {
					//System.out.println( "<funcao>" + e.select("span").get(0).text() + "</funcao>" );
					//System.out.println( "<nome>" + e.select("span").get(2).text() + "</nome>" );



					//StringBuffer sql = new StringBuffer();
					//sql.append( "INSERT INTO ds_jogador VALUES ( 44, '" + e.select("span").get(2).text() + "', '" + nomeClube + "', '" + e.select("span").get(0).text() + "', ");
					//sql.append( "                                  '" + "caract" + "') ");
					//System.out.println( sql );

					try {

						writer.print("0|");	//primeiro campo, Qlik nao pega
						writer.print( e.select("span").get(2).text() + "|" + nomeClube + "|" + e.select("span").get(0).text() + "|");
						//Elements caracteristicas = e.select("span[class=alignRight]");
						Elements caracteristicas = e.select("span[class=tar]");	//mudou, cacete!!!!
						StringBuffer dataCaracteristicas = new StringBuffer();
						for ( int ix = 0; ix<caracteristicas.size(); ix++) {
							String s = caracteristicas.get(ix).text(); 
							if ( e.select("span").get(0).text().equals("G")) {
								switch (ix) {
								case 1: dataCaracteristicas.append("<Idade>" + s + "</Idade>"); break; 
								case 2: dataCaracteristicas.append("<Jogos>" + s + "</Jogos>"); break; 
								case 4:	dataCaracteristicas.append("<Defesas>" + s + "</Defesas>"); break;
								case 5: dataCaracteristicas.append("<GolsSofridos>" + s + "</GolsSofridos>"); break; 
								case 6: dataCaracteristicas.append("<Assistencias>" + s + "</Assistencias>"); break;
								case 7: dataCaracteristicas.append("<Faltas>" + s + "</Faltas>"); break;
								case 8: dataCaracteristicas.append("<FaltasSofridas>" + s + "</FaltasSofridas>"); break;
								case 9: dataCaracteristicas.append("<CardsAmarelo>" + s + "</CardsAmarelo>"); break;
								case 10: dataCaracteristicas.append("<CardsVermelho>" + s + "</CardsVermelho>"); break;
								}
							} else { 
								switch (ix) {
								case 1: dataCaracteristicas.append("<Idade>" + s + "</Idade>"); break; 
								case 2: dataCaracteristicas.append("<Jogos>" + s + "</Jogos>"); break; 
								case 4:	dataCaracteristicas.append("<Gols>" + s + "</Gols>"); break;																								
								case 5: dataCaracteristicas.append("<Assistencias>" + s + "</Assistencias>"); break;
								case 6: dataCaracteristicas.append("<Finalizacoes>" + s + "</Finalizacoes>"); break;
								case 7: dataCaracteristicas.append("<FinalizacoesCertas>" + s + "</FinalizacoesCertas>"); break;
								case 8: dataCaracteristicas.append("<Faltas>" + s + "</Faltas>"); break;
								case 9: dataCaracteristicas.append("<FaltasSofridas>" + s + "</FaltasSofridas>"); break;
								case 10: dataCaracteristicas.append("<CardsAmarelo>" + s + "</CardsAmarelo>"); break;
								case 11: dataCaracteristicas.append("<CardsVermelho>" + s + "</CardsVermelho>"); break;
								}
							}
						}						
						writer.print( dataCaracteristicas.toString() );
						writer.println("|"+this.dsCampeonato.replace("/","").trim()+"|"+this.dsTemporada);
						//log.info("Temporada = " + this.dsTemporada);


					} catch (Exception sqle) { 
						sqle.printStackTrace();
					}
				}


				//stmt.close();
				//con.close();
				return (TreeMap<String, URL>)listaJogadores;
			}
		} catch (Exception sqle) {
			sqle.printStackTrace();
		}

		return null;
	}



}

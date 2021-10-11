package datamining;

import java.io.IOException;
import java.net.URL;
import java.util.TreeMap;
import java.util.TreeSet;

import igti.soccer.domain.Campeonato;

public class CargaDadosLigas {

	public static void main(String[] args) {

		getLigas();

	}

	private static void getLigas() {

		TreeSet<Campeonato> set = new TreeSet<>();
		//set.add( Campeonato.instance("http://www.espn.com.br/futebol/classificacao/_/liga/arg.1", "ESPN"));
		//set.add( Campeonato.instance("http://www.espn.com.br/futebol/classificacao/_/liga/por.1/season/2017", "ESPN"));


		
		set.stream().forEach( campeonato -> {
			System.out.println( campeonato.getDataSourceURL() );
			
			try {
				TreeMap<String, URL> listaClubes = campeonato.getClubes ();				
				listaClubes.forEach((k,v)-> {
					System.out.println ( "------------------------------------------------------" );
					System.out.println("key: " + k + ", value: " + v); 
					System.out.println ( "------------------------------------------------------" );
					
					try {						
						campeonato.getAtletas(k, v);
						
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




}

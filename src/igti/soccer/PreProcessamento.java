package igti.soccer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.mileto.util.Security;

import igti.soccer.datasource.DataSourceEspn;
import igti.soccer.domain.Campeonato;

/**
 * Esta fase vai processar os arquivos textos no formato HTML já hospedados no sistema de arquivos, pois já foram coletados da Web na fase anterior. 
 * @author abrhaao
 *
 */
public class PreProcessamento {

	private static final Pattern SPACE = Pattern.compile(" ");
	public static final Log log = LogFactory.getLog(PreProcessamento.class);
	public static final Path hiveDados =  Paths.get( new File("/tmp/hivedados/papli.db").getAbsolutePath() );

	public static void main ( String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("IGTI Projeto Aplicado - PreProcessamento")	      
				//.config("hive.metastore.uris", "thrift://localhost:9083")
				//.enableHiveSupport()
				//.master("local[*]")
				.master("spark://tricia:7077")
				.getOrCreate();

		/** Lé o arquivo de campeonatos **/
		//JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		/** Monta o RDD de campeonatos na sessão do Spark **/
		//JavaRDD<Campeonato> campeonatos = lines.flatMap(new FlatMapFunction<String, Campeonato>() {
		//	@Override public Iterator<Campeonato> call(String s) { 				
		//		return Arrays.asList(Campeonato.instance(s)).iterator(); 
		//	}
		//});

		/** Varre todos os arquivos coletados **/
		Path hiveDadosClubesColetados = Paths.get( hiveDados.toAbsolutePath().toString()+"/coletados/");
		File[] arquivos = hiveDadosClubesColetados.toFile().listFiles();
		Arrays.stream(arquivos).forEach(
				a -> {
					if ( a.isDirectory()) { 
						Arrays.stream(a.listFiles()).forEach( liga -> {	
							log.info(liga.getName());
							apagaJogadoresDestaLiga(liga);

							/** Identifica qual é o campeonato do arquivo coletado **/					
							JavaRDD<String> clubesRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(liga.getAbsolutePath(), 1);
							clubesRDD.collect().forEach( clube -> { 
								List<String> items = Arrays.asList(clube.split(","));
								log.info(items.get(0));
								log.info(items.get(1));								


								if ( a.getName().equals("ESPN") ) { 

									try { 
										
										
										PrintWriter writer = new PrintWriter( hiveDados.toAbsolutePath().toString() + "/players/" + "players_" +  liga.getName().replace(".", "") + Security.giveMeFuckingName(5)+ ".txt", "UTF-8");
										DataSourceEspn dataSource = new DataSourceEspn( liga.getName(), "2099" );
										dataSource.getAtletas( items.get(0).replace("(","") , new URL ( "file://" + items.get(1).replace(")","")), writer);
										writer.close();
									} catch ( IOException ioe ) { 
										ioe.printStackTrace();
									}


								}

							});


						});
					}
				}
				);



		/**
		+ campeonato.getDataSourcePortal() + "/" + campeonato.getSigla().replace("/", ""));

		JavaRDD<String> pairRDD = JavaSparkContext.fromSparkContext(spark.sparkContext()).textFile(hiveDadosClubesColetados.toAbsolutePath().toString(), 1);
		//JavaPairRDD<String, String> pairRDD = JavaPairRDD.fromJavaRDD();
		pairRDD.collect().forEach(log::info);


		 **/

		spark.stop();

	}

	
	private static void apagaJogadoresDestaLiga(File directoryLiga) {
		
		File directoryPlayers = new File ( hiveDados.toAbsolutePath().toString() + "/players" );
		
		for (File f: directoryPlayers.listFiles()) {
		    if (f.getName().matches( directoryLiga.getName().replace(".", "") )) {
		        f.delete();
		    }
		}
	}
}

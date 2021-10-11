package igti.soccer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import igti.soccer.domain.Campeonato;
import my.teste.JavaWordCount;
import scala.Tuple2;


public final class CargaDadosWeb {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static final Log log = LogFactory.getLog(CargaDadosWeb.class);
	public static final Path hiveDados =  Paths.get( new File("/tmp/hivedados/papli.db").getAbsolutePath() );


	public static void main(String[] args) {

		final boolean sparkRun = true; 

		if (args.length < 1) {
			System.err.println("Usage: IGTI Projeto Aplicado - Coleta <file>");
				System.exit(1);
		}


		/** Apaga resíduo de dados pré-processados **/
		try { 
			Arrays.stream(new File (hiveDados.toFile().getAbsolutePath()+"/coletados").listFiles()).forEach(File::delete);
		} catch (Exception e) {}

		if ( !hiveDados.toFile().exists() ) {
			hiveDados.toFile().mkdir();
		}

		if (sparkRun) {

			SparkSession spark = SparkSession.builder()
					.appName("IGTI Projeto Aplicado - Coleta")
					.config("spark.cores.max", "4")  
					//.config("hive.metastore.uris", "thrift://localhost:9083")
					//.enableHiveSupport()
					//.master("local[*]")	
					.master("spark://tricia:7077")
					.getOrCreate();
			
			//.master("spark://mint:7077")	//tem que ser o que aparece em URL na página Web
			
			
			
			JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
			
			JavaRDD<Campeonato> campeonatos = lines.flatMap(new FlatMapFunction<String, Campeonato>() {
				@Override public Iterator<Campeonato> call(String s) { 
					 Campeonato campeonato = Campeonato.instance(s);
					return Arrays.asList(campeonato).iterator(); 
				}
			});
			
		
			List<Campeonato> listaCampeonatos = campeonatos.collect();
			spark.stop();
			
			
			for ( Campeonato c: listaCampeonatos) {
				processaCampeonato ( c );
			} 
			
			
			
			

		} else { 

			try {
				Scanner scanner = new Scanner(new File(args[0]));
				while (scanner.hasNextLine()) {
					//processaCampeonato ( Campeonato.instance(scanner.nextLine()) );
				}
				scanner.close();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}




		}




	}



	/**
	 * Processa os campeonatos definidos no arquivo texto
	 * @param campeonato
	 */
	private static void processaCampeonato(Campeonato campeonato) { 

		final Log log = LogFactory.getLog(CargaDadosWeb.class);

		try {
			
			/**
			 * Recupera a URL onde eu identificaremos a URL de cada clube
			 */
			TreeMap<String, URL> listaClubes = campeonato.getClubes ();	

			SparkSession spark = SparkSession
					.builder()
					.appName("IGTI Projeto Aplicado - WebCrawiling")	      
					//.config("spark.sql.warehouse.dir", warehouseLocation)
					.config("hive.metastore.uris", "thrift://localhost:9083")
					.enableHiveSupport()
					//.master("local[*]")
					.master("spark://tricia:7077")
					.getOrCreate();
			List<String> listaClubesKeys = new ArrayList<String>(listaClubes.keySet());


			JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
			JavaRDD<String> rowRDD = ctx.parallelize(listaClubesKeys);

			log.info("ROWRDD >> " + rowRDD.first());




			/** Limpa as pastas de arquivos coletados **/
			/** Define a sigla, contiga no arquivo original, removendo todas as barras à esquerda **/
			Path hiveDadosClubesColetados = Paths.get( hiveDados.toAbsolutePath().toString()+"/coletados/" + campeonato.getDataSourcePortal() + "/" + campeonato.getSigla().replace("/", ""));								
			if ( !hiveDadosClubesColetados.toFile().exists() ) {
				try { 
					hiveDadosClubesColetados.toFile().mkdir();
				} catch ( Exception e) { 
					FileUtils.deleteDirectory(hiveDadosClubesColetados.toFile());
					hiveDadosClubesColetados.toFile().mkdir();
				}
			} else { 
				FileUtils.cleanDirectory(hiveDadosClubesColetados.toFile());
				FileUtils.deleteDirectory(hiveDadosClubesColetados.toFile());
			}

			Path hiveDadosClubesPaginas = Paths.get( hiveDados.toAbsolutePath().toString() + "/paginas/" + campeonato.getSigla().replace("/", ""));
			if ( !hiveDadosClubesPaginas.toFile().exists() ) {
				hiveDadosClubesPaginas.toFile().mkdir();
			} else { 
				FileUtils.cleanDirectory(hiveDadosClubesPaginas.toFile());
			}
			String hiveDadosClubesPaginasDir = hiveDadosClubesPaginas.toAbsolutePath().toString();	//Dentro do MapReduce não pode entrar o objeto Path


			/** Tentativa de iterar **/
			JavaPairRDD<String, String> counts = rowRDD.mapToPair(
					new PairFunction(){						
						public Tuple2 call(Object x){
							//log.info( "We got inside Tuple2 call " + x.getClass() );
							//JOptionPane.showMessageDialog(null, listaClubes.get(x).toString() );
							log.info( "------------------------------------------------------" );
							log.info( "key: " + x + ", value: " + listaClubes.get(x).toString() ); 

							String arquivoClube = new String("");
							DateFormat dateFormat = new SimpleDateFormat("MM/dd HH:mm:ss.SSS");
							Calendar calIni = Calendar.getInstance();


							try {
								Process proc = Runtime.getRuntime().exec( "curl " + listaClubes.get(x).toString()  );
								BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));	//Formato varia conforme Liga
								String line;
								StringBuffer htmlContent = new StringBuffer();
								while ((line = reader.readLine()) != null) {				
									htmlContent.append(line);
								}
								//log.info(htmlContent.toString());
								
								/********************************************************************/
								/**	Salva o arquivo HTML do clube 								**/
								/********************************************************************/
								Document docHtml = Jsoup.parse(htmlContent.toString() );								
								arquivoClube = hiveDadosClubesPaginasDir + "/" + x.toString().replace(" ", "") + ".html";
								arquivoClube = arquivoClube.replace("'", "") + ".html";
								PrintWriter writer = new PrintWriter(arquivoClube, "UTF-8");							
								writer.print( docHtml.outerHtml() );
								writer.close();


							} catch (IOException e) {}


							Calendar calEnd = Calendar.getInstance();


							log.warn("Inicio = " + dateFormat.format(calIni.getTime()));
							log.warn("Fim = " + dateFormat.format(calEnd.getTime()));

							return new Tuple2( x,  arquivoClube ); 
						}}); /**.reduceByKey(new Function2<Integer, Integer, Integer>(){
							public Integer call(Integer x, Integer y){ 
								JOptionPane.showMessageDialog(null, "Reduce by Key ");
								try {						
									//campeonato.getAtletas(x.toString(),  listaClubes.get(x));
									throw new IOException();

								} catch ( IOException e) {
									//e.printStackTrace();
								} finally { 

								}
								return y;
							}});**/
			if ( hiveDadosClubesColetados.toFile().exists() ) {
				FileUtils.cleanDirectory(hiveDadosClubesColetados.toFile());
				FileUtils.deleteDirectory(hiveDadosClubesColetados.toFile());
			}
			counts.repartition(1).saveAsTextFile( hiveDadosClubesColetados.toAbsolutePath().toString() );
			
			spark.stop();

			//log.warn("TAMANHO DO HASH MAP = " + clubes.size());

			/**
			 JavaPairRDD<String, Document> counts = rowRDD.mapToPair(
				      new PairFunction() {
				          public Tuple2 call(Object s) {
				        	  Document docHtml  = new Document("");
				        	  try { 
				        	  Process proc = Runtime.getRuntime().exec( "curl http://www.yahoo.com.br"  );
								BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));	//Formato varia conforme Liga
								String line;
								StringBuffer htmlContent = new StringBuffer();
								while ((line = reader.readLine()) != null) {				
									htmlContent.append(line);
								}
				        	 docHtml = Jsoup.parse(htmlContent.toString() );
				        	 PrintWriter writer = new PrintWriter(hiveDados.toAbsolutePath().toString() + "/paginas/" + "campeonato-" + s.toString() + ".html", "UTF-8");
				     		 writer.print( docHtml.outerHtml() );
				     		 writer.close();
				        	} catch (IOException e) {}
				            return new Tuple2(s, hiveDados.toAbsolutePath().toString() + "/paginas/" + "campeonato-" + s.toString() + ".html");
				          }
				        }
				      );
			 counts.repartition(1).saveAsTextFile( hiveDados.toAbsolutePath().toString()+"/mapreduce_"+ campeonato.getDataSourceURL().replaceAll("http://", ""));
			 **/       


			/** Isso funciona muito bem !!! 
			listaClubes.forEach((k,v)-> {
				log.info( "------------------------------------------------------" );
				log.info( "key: " + k + ", value: " + v); 
				log.info( "------------------------------------------------------" );

				try {						
					campeonato.getAtletas(k, v);

				} catch ( IOException e) {
					e.printStackTrace();
				} finally { 

				}


			});
			 **/
			
			
			
			
			
			
			


		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}





}
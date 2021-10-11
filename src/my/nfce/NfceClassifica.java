package my.nfce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class NfceClassifica {

	public static final Log log = LogFactory.getLog(NfceRun.class);
	static final Path pathNfce = Paths.get("/var/spool/spark/nfce");
	static final Path pathNfceProdutos = Paths.get("/datalake/mercado/hivedados.produtos");

	public static void main ( String[] a) { 

		try {

			
					rodaSpark( );



		} catch (Exception ioe) {
			ioe.printStackTrace();
		}
	}



	private static void rodaSpark(  ) throws IOException { 

		String warehouseLocation = "/tmp/hivedados";
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark Hive Example")	      
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.config("hive.metastore.uris", "thrift://localhost:9083")
				.enableHiveSupport()
				.master("local[*]")
				//.master("spark://mint:7077")
				.getOrCreate();

		JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());

		spark.sql("use mymercado");
		spark.sql("show create table produto");
		
		Dataset<Row> dataSetProdutos = spark.sql(" select nome from produto ");
		JavaRDD<Row> rddProdutos = dataSetProdutos.javaRDD();
		
		Queue<String> listaProdutos = new ConcurrentLinkedQueue<String>();
		//rddProdutos.foreach( produto -> {
		for ( Row registro: rddProdutos.collect()) {
			String produto = registro.toString().replace("[", "").replace("]", "");
			log.info( "PRODUTO AVALIDAO >>>>> " + produto );
			
			
			Scanner scanner = new Scanner( new File ( "/datalake/mercado/core_classificacao_grupos.properties" ) );
			//StringBuffer content = new StringBuffer();
			while (scanner.hasNextLine()) {
				String linha = scanner.nextLine();				
				String grupo = new String("OUTROS");
				if ( produto.toString().contains( linha.substring(5, 50).trim() )) { 					
					if ( linha.length() > 69 ) {
						grupo = linha.substring(50, 70);
					} else {
						grupo = linha.substring(50, linha.length());
					}
					log.info( "PRODUTO AVALIado >>>>> " + produto + "    >>>  grupo = " + grupo); //linha.length()) );				
					listaProdutos.add( "insert into produto values ( 4, '"+produto.toString()+"', '" + grupo.trim().replaceAll("^\\s+", "") + "' ) ");
					break;
				}
			}						
		}
		
		//spark.sql("drop table if exists produto");
		
		
		FileUtils.cleanDirectory(pathNfceProdutos.toFile());
		
		//StringBuffer sentenceCreateTable = new StringBuffer();
		//sentenceCreateTable.append("CREATE TABLE IF NOT EXISTS produto (eid BIGINT, nome STRING, grupo STRING ");
		//sentenceCreateTable.append(" ) STORED AS PARQUET LOCATION '/datalake/mercado/hivedados.produtos'  ");
		//spark.sql( sentenceCreateTable.toString() );
		
		log.info("CLAUSULAS = " + listaProdutos.size());
		for (String clausula: listaProdutos) {
			log.info(clausula);
			spark.sql(clausula);
		}
		
		spark.sql("SELECT * FROM produto").show(80);
		spark.stop();


	}

}

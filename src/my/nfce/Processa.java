package my.nfce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Processa {

	public static void main ( String[] a) { 

		try {
			final File folder = new File("/datalake/mercado");
			File[] listOfFiles = folder.listFiles();		

			for (int i = 0; i < listOfFiles.length; i++) {
				Scanner scanner = new Scanner(listOfFiles[i]);
				StringBuffer content = new StringBuffer();
				while (scanner.hasNextLine()) {
					content.append(scanner.nextLine());					
				}
				Document docHtml = Jsoup.parse(content.toString());
				List<ItemNota> itens = ItemNota.getItens( docHtml );
				
				rodaSpark( itens );
				
				scanner.close();
			}


			

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}



private static void rodaSpark( List<ItemNota> itens ) { 
	
	  String warehouseLocation = "/tmp/hivedados";
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("Java Spark Hive Example")	      
	      .config("spark.sql.warehouse.dir", warehouseLocation)
	      .config("hive.metastore.uris", "thrift://localhost:9083")
	      .enableHiveSupport()
	     // .master("spark://mint:7077")
	      .getOrCreate();
	    
	    JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
	    JavaRDD<ItemNota> rowRDD = ctx.parallelize(itens);
	    
	    
	    spark.sql("use mymercado");
	    spark.sql("drop table if exists produto");
	    
	    StringBuffer sentenceCreateTable = new StringBuffer();
	    sentenceCreateTable.append("CREATE TABLE IF NOT EXISTS produto (eid BIGINT, nome STRING, grupo STRING ");
	    sentenceCreateTable.append(") USING hive");
	   
	    spark.sql(sentenceCreateTable.toString());
	    
	    spark.sql("show create table produto");
	    
	    spark.sql("SELECT * FROM produto").show();
	    

	    spark.stop();
	
	
}

}


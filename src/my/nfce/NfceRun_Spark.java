package my.nfce;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
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

public class NfceRun_Spark {

	public static final Log log = LogFactory.getLog(NfceRun_Spark.class);
	public static final Path pathNfce = Paths.get("/var/spool/spark/nfce");
	static final Path pathNfceProdutos = Paths.get("/datalake/mercado/hivedados.produtos");
	static final Path pathNfceCompras = Paths.get("/datalake/mercado/hivedados.compras");

	public static void main ( String[] a) { 

		try {

			recria();

			final File folder = new File("/datalake/mercado");
			File[] listOfFiles = folder.listFiles();		

			for (int i = 0; i < listOfFiles.length; i++) {
				if ( listOfFiles[i].isFile() && listOfFiles[i].getAbsolutePath().contains(".html")) { 
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
				.master("local[*]")
				//.master("spark://mint:7077")
				.getOrCreate();

		JavaSparkContext ctx = new JavaSparkContext(spark.sparkContext());
		JavaRDD<ItemNota> rowRDD = ctx.parallelize(itens);

		spark.sql("use mymercado");
		spark.sql("show create table produto");


		StringBuilder sentenceInsertTable = new StringBuilder();
		sentenceInsertTable.append("INSERT INTO compra values ");
		//rowRDD.foreach( split -> {
		for ( ItemNota split: rowRDD.collect())  {

			
			sentenceInsertTable.append("('" + ((ItemNota)split).getDescricao() + "', " + ((ItemNota)split).getQuant().toString() + ", " + ((ItemNota)split).getPrecoUnitario().toString() + ", ");
			sentenceInsertTable.append(" "+ ((ItemNota)split).getValorTotal().toString() + ", '" + ((ItemNota)split).getEmissao() +  "'  ),");
			
			//log.info( split );

		}
		sentenceInsertTable.deleteCharAt( sentenceInsertTable.length()-1 );
		log.info( sentenceInsertTable.toString() );
		spark.sql(sentenceInsertTable.toString());

		try {
			FileUtils.cleanDirectory(pathNfce.toFile());
			FileUtils.deleteDirectory(pathNfce.toFile());
		} catch (IOException ioe){
			ioe.printStackTrace();
		}



		/** Isto agrupa o RDD por i.getDescricao(), dando quantas ocorrencias tem cada um **/
		JavaPairRDD<String, Integer> counts = rowRDD.mapToPair(
				new PairFunction(){
					public Tuple2 call(Object x){
						log.info( "We got inside Tuple2 call " + x.getClass() );
						ItemNota i = (ItemNota)x;
						return new Tuple2(i.getDescricao(), 1);
					}}).reduceByKey(new Function2<Integer, Integer, Integer>(){
						public Integer call(Integer x, Integer y){ 
							log.info( "We got inside Function2 call x " + x.getClass() );
							log.info( "We got inside Function2 call y" + x.getClass() );
							return x + y;
						}});

		/**
	    JavaPairRDD counts = rowRDD.mapToPair(w -> new Tuple2(w, 1))
	    	         .reduceByKey(( itemNota, y) -> {			//O primeiro parâmetro do reduceByKey é o objeto passado. 
	    						ItemNota itemReduce = (ItemNota)itemNota;	
	    	         			return itemReduce.getDescricao();	}							//O conteúdo da função é o critério de agrupamento
	    	         );	
		 **/
		counts.repartition(1).saveAsTextFile( pathNfce.toAbsolutePath().toString() );




		/**

	    JavaPairRDD<String, Integer> counts = rowRDD.mapToPair(
	    	      new PairFunction<ItemNota, String, Integer>(){
	    	        public Tuple2<String, Integer> call(ItemNota x){
	    	          return new Tuple2(x, 1);
	    	        }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
	    	            public Integer call(Integer x, Integer y){ return x + y;}});
	    	    // Save the word count back out to a text file, causing evaluation.
	    	    counts.saveAsTextFile("/var/spool/log/nfce-reducebykey");
	    	}
		 **/



		int indexProduto = 0;
		StringBuilder sentenceInsertProduto = new StringBuilder();
		sentenceInsertProduto.append("INSERT INTO produto values ");		
		for ( String split: counts.keys().collect() )  {
			sentenceInsertProduto.append("(" + new Integer(indexProduto++).toString() +", '"+split+"', null ) ,");			
		}		
		sentenceInsertProduto.deleteCharAt( sentenceInsertProduto.length()-1 );
		spark.sql(sentenceInsertProduto.toString());

		spark.sql("SELECT * FROM produto").show();
		spark.stop();


	}



	private static void recria() { 

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
		spark.sql("drop table if exists produto");
		spark.sql("drop table if exists compra");

		try { 
		FileUtils.cleanDirectory(pathNfceProdutos.toFile());
		FileUtils.cleanDirectory(pathNfceCompras.toFile());
		} catch (IOException ioe ) { 
			ioe.printStackTrace();
		}
		
		StringBuffer sentenceCreateTable = new StringBuffer();
		sentenceCreateTable.append("CREATE TABLE IF NOT EXISTS produto (eid BIGINT, nome STRING, grupo STRING ");
		sentenceCreateTable.append(" ) STORED AS PARQUET LOCATION '"+pathNfceProdutos.toAbsolutePath()+"'  ");
		//sentenceCreateTable.append( " ) stored as orc tblproperties(\"transactional\"=\"true\")" );
		spark.sql(sentenceCreateTable.toString());

		
		sentenceCreateTable = new StringBuffer();
		sentenceCreateTable.append(" CREATE EXTERNAL TABLE IF NOT EXISTS compra (produto STRING, quantidade DOUBLE, preco DOUBLE, total DOUBLE, quando STRING  ");
		sentenceCreateTable.append(" ) STORED AS PARQUET LOCATION '"+pathNfceCompras.toAbsolutePath()+"'  ");
		spark.sql(sentenceCreateTable.toString());

		spark.sql("show create table compra");
		spark.stop();


	}

}
/**
class ItemNota implements Serializable {

	private String descricao;
	private Double precoUnitario;
	private Double valorTotal;
	private Double quant;
	private String codigoItemMercado;
	private String estabelecimento;
	private String emissao;

	public ItemNota(Element elemento) { 
		setDescricao ( 			elemento.select("span[class=txtTit]").text() );
		setCodigoItemMercado( 	elemento.select("span[class=RCod]").text() );
		setQuant(  				elemento.select("span[class=Rqtd]")  ); //e.get(ix).select("span[class=Rqtd]");
		setPrecoUnitario( 		elemento.select("span[class=RvlUnit]"));
		setValorTotal(			elemento.select("span[class=valor]"));
		//e.get(ix).select("span[class=valor]");
	}

	public Double getQuant() {
		return quant;
	}

	public void setQuant(Double quant) {
		this.quant = quant;
	}

	public void setQuant(Elements elementoQuantidade ) {
		String tagQuant = elementoQuantidade.text();
		tagQuant = tagQuant.replaceAll("Qtde.:", "");
		this.quant = Double.parseDouble( tagQuant.replaceAll(",", ".") );
	}

	public static List<ItemNota> getItens(Document d) {
		List<ItemNota> lista = new ArrayList<ItemNota>();

		Elements e = d.select("tr[id~=Item*]");
		for ( int ix = 0; ix<e.size(); ix++) {
			ItemNota i = new ItemNota(e.get(ix));	
			i.setEstabelecimento(d.select("div[class=txtTopo]").text());
			
			String infos = d.select("div[id=infos]").text();
			String emissao = infos.substring(infos.indexOf("Emissão: ")+9, infos.indexOf("Emissão: ")+19);
			i.setEmissao(emissao);
			
			lista.add(i);
		}
		return lista;

	}

	public String getDescricao() {
		return descricao;
	}

	public void setDescricao(String descricao) {
		this.descricao = descricao;
	}

	public Double getPrecoUnitario() {
		return precoUnitario;
	}

	public void setPrecoUnitario(Double precoUnitario) {
		this.precoUnitario = precoUnitario;
	}

	private void setPrecoUnitario(Elements elementoPreco ) {
		String tagPreco = elementoPreco.text();
		tagPreco = tagPreco.replaceAll("Vl. Unit.: ", "");
		this.precoUnitario = Double.parseDouble( tagPreco.replaceAll(",", ".") );
	}

	public Double getValorTotal() {
		return valorTotal;
	}

	public void setValorTotal(Double valorTotal) {
		this.valorTotal = valorTotal;
	}
	
	private void setValorTotal(Elements elementoTotal ) {
		String tagPreco = elementoTotal.text();
		this.valorTotal = Double.parseDouble( tagPreco.replaceAll(",", ".") );
	}

	public String getCodigoItemMercado() {
		return codigoItemMercado;
	}

	public void setCodigoItemMercado(String codigoItemMercado) {
		if ( codigoItemMercado != null ) { 
			codigoItemMercado = codigoItemMercado.replace("Código: ", "");
			codigoItemMercado = codigoItemMercado.replace(")", "").replace("(", "");
			codigoItemMercado = codigoItemMercado.trim();
		}
		this.codigoItemMercado = codigoItemMercado;
	}

	public String getEstabelecimento() {
		return estabelecimento;
	}

	public void setEstabelecimento(String estabelecimento) {
		this.estabelecimento = estabelecimento;
	}

	public String getEmissao() {
		return emissao;
	}

	public void setEmissao(String emissao) {
		this.emissao = emissao;
	}



}
**/
package my.teste;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.*;


public class ExampleJob {

private static JavaSparkContext jsc;

    
    public ExampleJob(JavaSparkContext sc){
      this.jsc = sc;
      
   //   JavaSparkContext.toSparkContext(jsc);
    }
    
    public ExampleJob(SparkSession sc){
        
    	System.out.println( sc );
    	
    	//sc.sql("create database twilight;");
    	sc.sql("show databases").show();
      }
    
    
    public static void main(String[] args) throws Exception {
    	
    	String driverName = "org.apache.hive.jdbc.HiveDriver";
    			
        /**
    	JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJoins")
        		.setMaster("local").set("spark.sql.warehouse.dir", "/tmp").set("hive.metastore.uris", "thrift://localhost:9083")        		
        		); 
        **/
    	
    	/**
    	SparkSession sc = SparkSession.builder()
    			.appName("SparkJoins")
        		.master("local")        		
        		.config("spark.sql.warehouse.dir", "/tmp")
        		.config("hive.metastore.warehouse.dir", "/tmp")        		
        		.config("hive.metastore.uris", "thrift://localhost:9083")
        		.getOrCreate(); 
                
        ExampleJob job = new ExampleJob(sc);
        **/
       // JavaPairRDD<String, String> output_rdd = job.run(args[0], args[1]);
       // output_rdd.saveAsHadoopFile(args[2], String.class, String.class, TextOutputFormat.class);
        
        
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/my", "abrhaao", "java");
        Statement stmt = con.createStatement();
        ResultSet res;
        //String tableName = "testHiveDriverTable";
        
        StringBuffer sql = new StringBuffer();
        //sql.append( " CREATE TABLE IF NOT EXISTS customer ( custno int, nome String, vendas double ) COMMENT 'testes  para aprendizado do Hive' "); 
        //stmt.execute(sql.toString());
        
        
        sql = new StringBuffer();
        //sql.append( "INSERT INTO customer (custno, nome, vendas) VALUES ( 44, 'Dualio Modric', 243.97 )  "); 
        //stmt.executeUpdate(sql.toString());
        
        //System.out.println("Running: " + sql);
         //stmt.executeQuery(sql.toString());

                
        sql = new StringBuffer ( "show databases" );
        System.out.println("Running: " + sql.toString() );
        res = stmt.executeQuery(sql.toString());
        while (res.next()) {
          System.out.println(res.getString(1));
        }
        
        
        List<String> paralela = new ArrayList(); 
        
        sql = new StringBuffer ( "select nome from my.ds_liga" );
        System.out.println("Running: " + sql.toString() );
        res = stmt.executeQuery(sql.toString());
        while (res.next()) {
          System.out.println(res.getString(1));
          paralela.add ( res.getString(1) );
        }
        
        
        
        
        
        
               
        
        
        
        
        
        
        
        
        
		//sql = new StringBuffer ( "select * from clicks_json" );
		//System.out.println("Running: " + sql.toString() );
		//res = stmt.executeQuery(sql.toString());
		//while (res.next()) {
		 // System.out.println(res.getString(1));
		//}
        
        SparkSession sc = SparkSession.builder()
    			.appName("SparkJoins")
    		      .master("spark://mint:7077")	//tem que ser o que aparece em URL na página Web
    		      .config("spark.cores.max", "4")              		        		
        		.getOrCreate(); 
        
        JavaSparkContext jsc = new JavaSparkContext(sc.sparkContext());
        JavaRDD<String> dados = jsc.parallelize(paralela);
        
        System.out.println("* JAVARDD >>>>>>>> TAMANHO "+dados.count());
        /**
        dados.foreach(new VoidFunction<String>(){ 
            public void call(String line) {
                System.out.println("* JAVARDD >>>>>>>> "+line); 
            }
         });**/
        
        JavaRDD<List<String>> counts = dados.map(new Function <String, List<String>>() { //line 43
            @Override
            public List<String> call(String s) {
            	System.out.println(">>>>>>>>>>>>>>>>>>> INDISE MAP.DADOS " + s);
              return Arrays.asList(s.split("\\s*,\\s*"));
            }
          }); 
        

        
        
        
        
        
        
        
        
        
        
        
        Dataset<Row> dataLigas = sc.read().format("jdbc")
        .option("url", "jdbc:hive2://localhost:10000/my")
        .option("dbtable", "my.ds_liga")
        .load();
        //dataLigas.show();
        //dataLigas.
        
        //Dataset<Row> use = sc.sql("use my");
        //use.show(); //this should print the database list.
        
        //Dataset<Row> df = sc.sql("select * from my.ds_liga");
        //df.show(); //this should print the database list.

        //Dataset<Row> dff = sc.sql("SHOW TABLES");
        //dff.show(); //this should print all table list.
        
        //Dataset<Row> sqlDF = sc.sql("SELECT * FROM clicks_json");
        //sqlDF.show();
        
        //sc.printSchema();
        //println(sc.count());
        //sc.show();
        sc.close();
    }
}
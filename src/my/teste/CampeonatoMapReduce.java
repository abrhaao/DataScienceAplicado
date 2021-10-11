/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package my.teste;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import igti.soccer.domain.Campeonato;



public final class CampeonatoMapReduce {
  private static final Pattern SPACE = Pattern.compile(" ");
  
  public static final Log log = LogFactory.getLog(JavaWordCount.class);

  public static void main(String[] args) {

	 final boolean sparkRun = false; 
	 
    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder()
      .appName("JavaWordCount")
     //.master("spark://mint:7077")	//tem que ser o que aparece em URL na página Web
      .config("spark.cores.max", "2")      
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    
    if (sparkRun) { 
	    JavaRDD<String> campeonatos = lines.flatMap(new FlatMapFunction<String, String>() {
	        @Override public Iterator<String> call(String s) { 
	        	System.out.println( ">>>>>>>>>> Estamos dentro do flatMap() " + s );
	        	System.out.println ( "Aqui dece ocorrer o processamento de todo este campeonato");
	        	processaCampeonato ( Campeonato.instance(s) );
	        	return Arrays.asList(SPACE.split(s)).iterator(); 
	        }
	    });
	    campeonatos.collect();
    } else { 
    
    	try {
    	Scanner scanner = new Scanner(new File(args[0]));
		while (scanner.hasNextLine()) {
			processaCampeonato ( Campeonato.instance(scanner.nextLine()) );
		}
		scanner.close();
    	} catch (IOException ioe) {
    		ioe.printStackTrace();
    	}
    	
    	
    	
    	
    }
    


    /**
    JavaPairRDD<String, Integer> ones = words.mapToPair(
    		new PairFunction<String, String, Integer>() {
    	        @Override
    	        public Tuple2<String, Integer> call(String s) throws Exception {
    	        	System.out.println( ">>>>>>>>>> Estamos dentro do mapToPair() >> " + s );
    	        	return new Tuple2<String, Integer>(s, 1);
    	        }
    	});
    **/

    /**
    JavaPairRDD<String, Integer> counts = ones.reduceByKey( 
    		
    		 new Function2<Integer, Integer, Integer>() {
    		        @Override
    		        public Integer call(Integer a, Integer b) throws Exception {
    		          return a + b;
    		        }
    		}
    );	**/	
    
    /**
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    **/
    spark.stop();
    
  }
  
  
  
  
  private static void processaCampeonato(Campeonato campeonato) { 
	  
	  final Log log = LogFactory.getLog(CampeonatoMapReduce.class);
	  
	  try {
			TreeMap<String, URL> listaClubes = campeonato.getClubes ();				
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

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
  }
  
  
}

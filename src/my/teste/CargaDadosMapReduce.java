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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;



public final class CargaDadosMapReduce {
  private static final Pattern SPACE = Pattern.compile(" ");
  
  public static final Log log = LogFactory.getLog(JavaWordCount.class);

  public static void main(String[] args) {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder()
      .appName("JavaWordCount")
      //.master("spark://mint:7077")	//tem que ser o que aparece em URL na página Web
      .config("spark.cores.max", "2")      
      .getOrCreate();

    //Cada linha do arquivo será o endereço Web de uma liga diferente
    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    
    
    
    

    //JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        @Override public Iterator<String> call(String s) { 
        	System.out.println( ">>>>>>>>>> Estamos dentro do flatMap()" );
        	return Arrays.asList(SPACE.split(s)).iterator(); 
        }
    });
    


    //JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairRDD<String, Integer> ones = words.mapToPair(
    		new PairFunction<String, String, Integer>() {
    	        @Override
    	        public Tuple2<String, Integer> call(String s) throws Exception {
    	        	System.out.println( ">>>>>>>>>> Estamos dentro do mapToPair()" );
    	        	return new Tuple2<String, Integer>(s, 1);
    	        }
    	});

    //JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    JavaPairRDD<String, Integer> counts = ones.reduceByKey( 
    		
    		 new Function2<Integer, Integer, Integer>() {
    		        @Override
    		        public Integer call(Integer a, Integer b) throws Exception {
    		          return a + b;
    		        }
    		}
    );		
    
    
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    spark.stop();
    
  }
}

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
package igti.soccer;

// $example on:spark_hive$
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off:spark_hive$

/**
 * Fase de processamento e persistência para o DW
 * @author abrhaao
 *
 */
public class Armazenamento {

  // $example on:spark_hive$
  public static class Record implements Serializable {
    private int key;
    private String value;

    public int getKey() {
      return key;
    }

    public void setKey(int key) {
      this.key = key;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }
  // $example off:spark_hive$

  public static void main(String[] args) {
    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    //String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
	  String warehouseLocation = "/tmp/hivedados";
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate();

    
    spark.sql("use papli");
    spark.sql("drop table if exists dw_jogador");
    
    StringBuffer sentenceCreateTable = new StringBuffer();
    sentenceCreateTable.append("CREATE TABLE IF NOT EXISTS dw_jogador (eid BIGINT, nome STRING, clube STRING, posicao STRING, idade INT, "); 
    sentenceCreateTable.append(" jogos INT, gols INT, assistencias INT, finalizacoes INT, finalizacoes_certas INT, faltas_sofridas INT, ");
    sentenceCreateTable.append(" cards_amerlo INT, cards_vermelho INT, liga STRING, temporada INT ");
    sentenceCreateTable.append(") USING hive");       
    spark.sql(sentenceCreateTable.toString());
    
    spark.sql("show create table dw_jogador");
    
    
    
    
    
    StringBuffer sentenceInsertTable = new StringBuffer();
    //sentenceInsertTable.append("INSERT into dw_jogador ( select 33, nome, clube, posicao, 99,  "); 
    //sentenceInsertTable.append(" instr ( atributos, 'Jogos' ),   instr ( atributos, 'Gols' ),  instr ( atributos, 'Assistencias' ), ");
    //sentenceInsertTable.append(" instr ( atributos, 'Finalizacoes' ), instr ( atributos, 'FinalizacoesCertas' ), instr ( atributos, 'FaltasSofridas' ), ");
    //sentenceInsertTable.append(" instr ( atributos, 'CardsAmarelo' ), instr ( atributos, 'CardsVermelho' ) from arch_jogador ) ");
    
    
    sentenceInsertTable.append("INSERT into dw_jogador ( select 33, nome, clube, posicao, "); 
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<Idade>' ) + length('<Idade>'), instr ( atributos, '</Idade>' )-length('</Idade>')), ");
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<Jogos>' ) + length('<Jogos>'), instr ( atributos, '</Jogos>' )-instr ( atributos, '<Jogos>' )-length('</Jogos>')+1), ");  
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<Gols>' ) + length('<Gols>'), instr ( atributos, '</Gols>' )-instr ( atributos, '<Gols>' )-length('</Gols>')+1),     ");
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<Assistencias>' ) + length('<Assistencias>'), instr ( atributos, '</Assistencias>' )-instr ( atributos, '<Assistencias>' )-length('</Assistencias>')+1), ");      
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<Finalizacoes>' ) + length('<Finalizacoes>'), instr ( atributos, '</Finalizacoes>' )-instr ( atributos, '<Finalizacoes>' )-length('</Finalizacoes>')+1),   ");
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<FinalizacoesCertas>' ) + length('<FinalizacoesCertas>'), instr ( atributos, '</FinalizacoesCertas>' )-instr ( atributos, '<FinalizacoesCertas>' )-length('</FinalizacoesCertas>')+1), ");   
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<FaltasSofridas>' ) + length('<FaltasSofridas>'), instr ( atributos, '</FaltasSofridas>' )-instr ( atributos, '<FaltasSofridas>' )-length('</FaltasSofridas>')+1),   ");
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<CardsAmarelo>' ) + length('<CardsAmarelo>'), instr ( atributos, '</CardsAmarelo>' )-instr ( atributos, '<CardsAmarelo>' )-length('</CardsAmarelo>')+1),   ");
    sentenceInsertTable.append(" substr ( atributos, instr ( atributos, '<CardsVermelho>' ) + length('<CardsVermelho>'), instr ( atributos, '</CardsVermelho>' )-instr ( atributos, '<CardsVermelho>' )-length('</CardsVermelho>')+1),  ");
    sentenceInsertTable.append(" liga, temporada ");
    sentenceInsertTable.append(" from arch_jogador ) ");

    spark.sql(sentenceInsertTable.toString());
    
    
    
    //spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM dw_jogador").show();
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM dw_jogador").show();
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    /**
    Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

    // The items in DataFrames are of type Row, which lets you to access each column by ordinal.
    Dataset<String> stringsDS = sqlDF.map(
        (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
        Encoders.STRING());
    stringsDS.show();


    List<Record> records = new ArrayList<>();
    for (int key = 1; key < 100; key++) {
      Record record = new Record();
      record.setKey(key);
      record.setValue("val_" + key);
      records.add(record);
    }
    Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
    recordsDF.createOrReplaceTempView("records");

    // Queries can then join DataFrames data with data stored in Hive.
    spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // ...
    // $example off:spark_hive$
**/
    spark.stop();
    
  }
}

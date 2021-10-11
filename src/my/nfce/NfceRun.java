package my.nfce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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

public class NfceRun {

	public static final Log log = LogFactory.getLog(NfceRun.class);
	public static final Path pathNfce = Paths.get("/var/spool/spark/nfce");
	static final Path pathNfceProdutos = Paths.get("/datalake/mercado/hivedados.produtos");
	static final Path pathNfceCompras = Paths.get("/datalake/mercado/hivedados.compras");
	static final Path pathNfceFiles = Paths.get("/datalake/mercado/");

	public static void main ( String[] a) { 

		try {

			//recria();

			PrintWriter writer = new PrintWriter( new FileOutputStream( new File( pathNfceFiles.toAbsolutePath() + "/compras.csv"), true));
			final File folder = new File("/datalake/mercado");
			File[] listOfFiles = folder.listFiles();		

		    //geraCSV_Header( writer );
			for (int i = 0; i < listOfFiles.length; i++) {
				if ( listOfFiles[i].isFile() && listOfFiles[i].getAbsolutePath().contains(".htm")) { 
					Scanner scanner = new Scanner(listOfFiles[i]);
					StringBuffer content = new StringBuffer();
					while (scanner.hasNextLine()) {
						content.append(scanner.nextLine());					
					}
					Document docHtml = Jsoup.parse(content.toString());
					List<ItemNota> itens = ItemNota.getItens( docHtml );
					scanner.close();
					
					geraCSV( itens, writer );					
				}
			}
			writer.close();




		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}


	private static void geraCSV( List<ItemNota> itens, PrintWriter writer ) {

		try {
			StringBuilder sb = new StringBuilder();
			for ( ItemNota item: itens ) {				
				sb.append( item.getEmissao()+";");
				sb.append( item.getCodigoItemMercado()+";");
				sb.append( item.getDescricao()+";");
				sb.append( item.getQuant()+";");
				sb.append( item.getPrecoUnitario()+";");
				sb.append( item.getValorTotal()+";");
				sb.append( item.getEstabelecimento()+";");
				sb.append('\n');				
			}	
			writer.append(sb.toString());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}


	
	private static void geraCSV_Header( PrintWriter writer ) { 

		StringBuilder sb = new StringBuilder();				
			sb.append( "EMISSAO"+";");
			sb.append( "ITEM"+";");
			sb.append( "DESCRICAO"+";");
			sb.append( "QUANTIDADE"+";");
			sb.append( "PRECO"+";");
			sb.append( "TOTAL"+";");
			sb.append( "ESTABELECIMENTO"+";");
			sb.append('\n');				
			writer.append(sb.toString());
	}	
		


}

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

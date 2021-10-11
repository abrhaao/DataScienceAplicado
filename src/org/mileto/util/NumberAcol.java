package org.mileto.util;

/**
 * Faz todos os tratamentos relacionados a n�meros.
 * @author Luiz Alberto Sodr�
 */
 
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Locale;


public class NumberAcol {
  public static final int BRASIL = 1;
  public static final int USA    = 2;
  
  private static String DMN_CUR_REAL  = "R$";
  private static String DMN_CUR_DOLAR = "US$";
    
  /**
   * Se a String recebida for nula, retorna a String retornada
   * @param strRecebida   String a ser verificada
   * @param strRetornada  String a ser retornada em caso da primeira ser nula
   * @return  A String recebida, se esta for diferente de nulo. String Retornada, caso contrario.
   * @throws java.lang.NumberFormatException Lan�ada quando a String a ser verificada n�o � um n�mero inteiro
   */
  public static int nvl(String pStrRecebida, int pIntRetornada) throws NumberFormatException {
    if ((pStrRecebida == null) || (pStrRecebida.equals("null")) || (pStrRecebida.equals(""))) {
        return pIntRetornada;
    } else {
        return Integer.parseInt(pStrRecebida);
    }
  }
  
  /**
   * Se o objeto Double passado contiver valor nulo, retorno o valor primitivo double 0.
   * Caso contr�rio, retorna o tipo primitivo double associado.
   * @param pDblRecebido
   * @param pDlrRetornado
   * @return double
   * @throws NumberFormatException
   */
  public static double nvl(Double pDblRecebido, double pDblRetornado) throws NumberFormatException {
    if (pDblRecebido == null) {
        return pDblRetornado;
    } else {
        return pDblRecebido.doubleValue();
    }
  }
  
  /** Troca virgula (',') por ponto ('.') */
  public static String trocaVirgula(String texto) {
  	if (texto != null) {
  		String temp = new String();
  		
  		for (int n=0; n<texto.length();) {
  			char c = texto.charAt(n);
  			
  			if (c == ',') {
  				temp = temp + ".";
  			} else {
  				if (c=='.') {
  					n++;
  					temp = temp + texto.charAt(n);
  				} 
  				else {
  					temp = temp + c;
  				}
  			}
  			n++;
  		}
  		
  		return temp;
  	}
  	return null;
  }
  
  /** Tira a m�scara, colocando no formato a ser usado pelo double.
      Verifica a localidade da m�quina e dependendo de qual seja,
      troca ponto ('.') por virgula (',')
      ou virgula (',') por ponto ('.'). */
  public static String formataDouble(String texto){
      if (Locale.getDefault().toString().equals("pt_BR")) {
          return trocaVirgula(texto);
      } else {
          return trocaPonto(texto);
      }
  }

  /** Troca ponto ('.') por virgula (',') */
  public static String trocaPonto(String texto){
  	String temp = new String();
  	
  	for (int n=0; n<texto.length();){
  		char c = texto.charAt(n);
  		if(c == '.'){
  			temp = temp + ",";
  		} else{
  			if(c==','){
  				n++;
  				temp = temp + texto.charAt(n);
  			} else temp = temp + c;
  		}
  		n++;
  	}
  	return temp;
  }

  /** Muda do formato 1,000.00 para o formato 1000.00 usado por double */
  public static String formataPontoDbl(String texto){
    String temp = new String();

    for (int n=0; n<texto.length();){
      char c = texto.charAt(n);
      if(c == '.'){
        temp = temp + c;
      }else{
        if(c==','){
          n++;
          temp = temp + texto.charAt(n);
        } else temp = temp + c;
      }
      n++;
    }
    return temp;
  }

  /** Troca v�rgula (',') por ponto ('.') e retorna Double */
  public static Double trocaVirgulaDbl(String texto) throws NumberFormatException {
    if (texto.equals("")) return new Double(0);
    else {
      String temp = new String();

      for (int n=0; n<texto.length();){
        char c = texto.charAt(n);
        if(c == ','){
          temp = temp + ".";
        } else{
          if(c=='.'){
            n++;
            temp = temp + texto.charAt(n);
          } else temp = temp + c;
        }
        n++;
      }
      return new Double(temp);
    }
  }
  
   public static double tiraFormatoDbl(String pStrValor) {
     return Double.parseDouble(trocaVirgula(StringAcol.trocaBranco(StringAcol.nvl(pStrValor, "0"), "0")));
   }
   
  /**
   * Trunca um n�mero de acordo com o n�mero de casas passado
   * como par�metro
   * @param pDblNumber    N�mero a ser truncado
   * @param pIntNumCasas  N�mero de casas
   * @return              N�mero truncado
   */
  public static double trunca(double pDblNumber, int pIntNumCasas) {
    double fator    = Math.pow(10, pIntNumCasas);     
    double valorAux = pDblNumber * fator;
    valorAux        = Math.floor(valorAux);
    
    return          valorAux / fator;
  }
  
  /**
   * Trunca um n�mero de acordo com o n�mero de casas passado
   * como par�metro       
   * @param pBgdNum       N�mero a ser truncado
   * @param pIntNumCasas  N�mero de casas
   * @return              N�mero truncado
   */
  public static BigDecimal trunca(BigDecimal pBgdNum, int pIntNumCasas) {
    double fator    = Math.pow(10, pIntNumCasas);     
    double valorAux = pBgdNum.doubleValue() * fator;
    valorAux        = Math.floor(valorAux);
    valorAux        = valorAux / fator;
    
    return new BigDecimal(valorAux);
  }

  /**
   * Arredonda valores de acordo com o numero de casas decimais passados por parametro
   * @param plnNum             double a ser arredondado
   * @param pIntCasasDecimais  int numero de casas decimais
   * @return  o valor arredondado.
   */
  public static double arredonda(double plnNum, int pIntCasasDecimais) {
  	double lnMult = Math.pow(10,pIntCasasDecimais);
	return (Math.round(plnNum * lnMult)/(lnMult));
  }
  
  /**
   * Trunca um n�mero de acordo com o n�mero de casas passado
   * como par�metro
   * @param pStrNumber    N�mero a ser truncado
   * @param pIntNumCasas  N�mero de casas
   * @return              N�mero truncado
   */
  public static double trunca(String pStrNumber, int pIntNumCasas) {
    double fator    = Math.pow(10, pIntNumCasas);     
    double valorAux = Double.parseDouble(pStrNumber);
    valorAux        = valorAux * fator;
    valorAux        = Math.floor(valorAux);
    
    return          valorAux / fator;
  }

  /**
   * Retorna uma string em formato de moeda em real ou dolar. Caso n�o reconhe�a a localidade retorna null.
   * @param valor         Valor a ser formatado
   * @param localidade    1 para Real e 2 para Dolar
   * @param casasDecimais N�mero de casas decimais
   * @return String em formato de moeda
   */
  public static String formataParaMoeda$(double valor, int localidade, int casasDecimais) {
   
    switch (localidade) {
      case 1:
        return DMN_CUR_REAL + " " + formataParaMoeda(valor,localidade,casasDecimais);
      case 2:
        return DMN_CUR_DOLAR + " " + formataParaMoeda(valor,localidade,casasDecimais);
      default: 
        return null;
    }
  }
  
   /**
   * Retorna uma string em formato de moeda em real ou dolar. Caso n�o reconhe�a a localidade retorna null.
   * @param valor         Valor a ser formatado
   * @param localidade    1 para Real e 2 para Dolar
   * @param casasDecimais N�mero de casas decimais
   * @return String em formato de moeda
   */
  public static String formataParaMoeda(double valor, int localidade, int casasDecimais) {
    DecimalFormat formato;
    Locale moedaUs = new Locale("en", "US");
    Locale moedaBr = new Locale("pt", "BR");

    switch (localidade) {
      case 1:
        formato = (DecimalFormat)DecimalFormat.getInstance(moedaBr);
        formato.getDecimalFormatSymbols().setDecimalSeparator(',');
        formato.getDecimalFormatSymbols().setGroupingSeparator('.');
        formato.setMaximumFractionDigits(casasDecimais);
        formato.setMinimumFractionDigits(casasDecimais);
        return  formato.format(valor);
      case 2:
        formato = (DecimalFormat)DecimalFormat.getInstance(moedaUs);
        formato.getDecimalFormatSymbols().setDecimalSeparator(',');
        formato.getDecimalFormatSymbols().setGroupingSeparator('.');
        formato.setMaximumFractionDigits(casasDecimais);
        formato.setMinimumFractionDigits(casasDecimais);
        return formato.format(valor);
      default: 
        return null;
    }
  }
  
  /**
   * M�todo respons�vel por pegar o valor 100001 e 
   * tranforma-lo em Double do tipo 1000.01
   * @param vlrSemPontoSemVirgula
   * @param casasDecimais
   * @return 
   */
  public static Double transformaDouble(String vlrSemPontoSemVirgula, int casasDecimais){
    String vlrTransformado = vlrSemPontoSemVirgula.substring(0, vlrSemPontoSemVirgula.length() - casasDecimais ) +
                             '.' + vlrSemPontoSemVirgula.substring(vlrSemPontoSemVirgula.length() - casasDecimais, vlrSemPontoSemVirgula.length());
    return new Double (vlrTransformado);
  }
  
  /**
   * Este m�todo sofre com o problema de receber um valor Double maior do que 10 milh�es,
   * e vir em forma de nota��o cient�fica. Para este caso, deve ser aplicado o DecimalFormat
   * para resgatar a nota��o num�rica convencional e aplicar os m�todos troca v�rgula e a 
   * formata��o sem pontos, para arquivo de interface.
   * @param vlrDouble
   * @param casasDecimais
   * @return nota��o para arquivos texto, sem marca de v�rgula ou ponto
   */
  public static String transformaString(Double vlrDouble, int casasDecimais) {
  	
  	String vlrString;
  	if (vlrDouble.doubleValue() >= 10000000) {
  		DecimalFormat decimal = new DecimalFormat("#.00");
  		vlrString = decimal.format(arredonda(vlrDouble.doubleValue(), casasDecimais));
  		vlrString = trocaVirgula(vlrString);
  	} else {
  		vlrString = new Double(arredonda(vlrDouble.doubleValue(), casasDecimais)).toString();
  	}
  	
  	char c = '@';
  	int n;
  	for (n=0; c!='.'; n++) {
  		c = vlrString.charAt(n);
  	}
  	return vlrString.substring(0,n) + StringAcol.rPad(vlrString.substring(n, vlrString.length()), 2, "0");
  }
  
  /**
   * Elimina tanto os pontos como as v�rgulas de um objeto Double ou String
   * @param vlrComPontoComVirgula
   * @return string sem pontos e sem v�rgulas
   */
  public static String eliminaPontoVirgula(Object vlrComPontoComVirgula) {
  	String texto = vlrComPontoComVirgula.toString();
  	StringBuffer strSemPontoSemVirgula = new StringBuffer();
  	for (int n=0; n<texto.length(); n++) {
  		char c = texto.charAt(n);
  		if (c != '.' && c != ',') {
  			strSemPontoSemVirgula.append(c);
  		}
  	}
  	return strSemPontoSemVirgula.toString();
  }
  
}

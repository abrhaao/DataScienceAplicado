package org.mileto.util;

import java.util.StringTokenizer;

/**
 * Faz todos os tratamentos relacionados a String.
 * @author Nelson Cravo Junior
 * @date 02/11/2004
 */
public class StringAcol  {

    /**
     * Inicializa a classe.
     */
    public StringAcol() {
    }

  /**
   * Insere uma string na posição dada de outra string.
   * @param pStrDado  String original
   * @param pStrAppend  Dado a ser adicionado
   * @param pIntPosicao Posição na qual o dado será adicionado
   * @return A string com o dado incluido
   */
  public static String append(String pStrDado, String pStrAppend, int pIntPosicao){
      pStrDado = pStrDado.substring(0, pIntPosicao) + pStrAppend + pStrDado.substring(pIntPosicao, pStrDado.length());

      return pStrDado;
  }

  /**
   * Se a String recebida for vazia, retorna a String retornada.
   * @param strRecebida   String a ser verificada
   * @param strRetornada  String a ser retornada em caso da primeira ser nula
   * @return  A String recebida, se esta for diferente de string vazia. String Retornada, caso contrario.
   */
  public static String trocaBranco(String strRecebida, String strRetornada){
    if (strRecebida != null) {
      if("".equals(strRecebida))
        return strRetornada;
      else
        return strRecebida;
    }
    return null;
  }
  
  /** Preenche uma string com o caracter passado até o tamanho especificado para a esquerda. */
  public static String lPad(String pStrNome, int pNTamanho, String pStrCarac){
    String lStrNome = "";

    int lIntTamanhoNome = nvl(pStrNome, "").length();
    
    int lIntDif = pNTamanho - lIntTamanhoNome;

    for (int i = 0; i < lIntDif; i++) 
    {
        lStrNome += pStrCarac;
    }

    lStrNome += pStrNome;

    return lStrNome;
  }

  /** 
   * Preenche uma string com o caracter passado até o tamanho especificado para a direita. 
   * Se pBlnWithMaxLenth for true, será retornado uma String com tamanho máximo 
   * de acordo com pNTamanho. 
   * @param   pStrNome          String a ser formatada
   * @param   pNTamanho         quantidade de caracteres a serem completados
   * @param   pStrCarac         caracteres a serem completados
   * @param   pBlnWithMaxLenth  se true será retornado uma String com tamanho máximo de acordo com pNTamanho
   * @return  A String formatada
   */
  public static String rPad(String  pStrNome, 
                            int     pNTamanho, 
                            String  pStrCarac) {
    int lNDiferencaTam;

    lNDiferencaTam = pNTamanho - nvl(pStrNome,"").length();

    while (0 < lNDiferencaTam ) {
      pStrNome = pStrNome + pStrCarac;
      lNDiferencaTam --;
    }
    
    pStrNome = pStrNome.substring(0, pNTamanho); 
    
    return pStrNome;
  }

  /**
   * Se a String recebida for nula, retorna a String retornada
   * @param strRecebida   String a ser verificada
   * @param strRetornada  String a ser retornada em caso da primeira ser nula
   * @return  A String recebida, se esta for diferente de nulo. String Retornada, caso contrario.
   */
  public static String nvl(String strRecebida, String strRetornada){
    if ((strRecebida == null) || (strRecebida.equals("null")))
      return strRetornada;
    else
      return strRecebida;
  }
  
  /** Retorna o numero de vezes que p existe em str */
  public static int inStr(String str, char p){
    int ps = 0;
    for(int i=0; i<str.length(); i++) {
      if(str.charAt(i) == p)
        ps++;
    }
    return ps;
  }
  
  
  /**
   * Monta o telefone no seguinte formato: +DDI (DDD) numero 'Ramal: ' ramal
   * @param pStrDDI
   * @param pStrDDD
   * @param pStrNumero
   * @param pStrRamal
   * @return Telefone formatado
   */
  public static String montaTelefone( String	pStrDDI,    String	pStrDDD, 
                                      String	pStrNumero, String	pStrRamal ) {
      String	lStrTelefone = pStrNumero;
      if (pStrDDD != null) {
        if (! pStrDDD.equals("")) {
          lStrTelefone = "(" + pStrDDD + ")" + lStrTelefone;
        }
      }
      if (pStrDDI != null) {
        if (! pStrDDI.equals("")) {
          lStrTelefone = "+" + pStrDDI + " " + lStrTelefone;
        }
      }
      if (pStrRamal != null) {
        if (! pStrRamal.equals("")) {
          lStrTelefone += " Ramal: " + pStrRamal;
        }
      }
      
      return lStrTelefone;
  }
  
  /**
   * Preenche o formato da conta com zeros,
   * ex: se o formato é 9.9.9.9.99.999, e
   * é passado 6.1, ele retorna 6.1.0.0.00.000
   */
  public static String aclStrSetPreenchido(String formato, String xstr) {
    String formada = xstr;
    int n=0;
    try{
      for(int i=formada.length(); i<formato.length(); i++) {
        if(formato.charAt(i)=='.') {
          formada = formada + formato.charAt(i);
        } else {
          formada = formada + "0";
        }
      }
    } catch(StringIndexOutOfBoundsException e){
      System.out.println("formato: " + formato + " xstr: " + xstr);
    }
    return(formada);
  }

  /**
   * Passa um numero para o formato passado, ex:
   * Se é passado o formato 9.9.9.9.99.999, e o número
   * 612400000, ele retorna 6.1.2.4.00.000
   */
  public static String aclStrSetFormat(String formato, String xstr) {
    String formada = "";
    int n=0;
    try {
      for(int i=0; i<formato.length(); i++) {
        if(formato.charAt(i)=='.') {
          formada = formada + formato.charAt(i);
        } 
        else {
          formada = formada + xstr.charAt(n);
          n++;
        }
      }
      return formada;
    } 
    catch(StringIndexOutOfBoundsException e) {
        return formada;
    }
  }

  /**
   * Remove os zeros que estão acima do grau da conta e retorna a string formatada.
   * Exemplo: entradas = 9.9.9.9.99.999, 113210000
   * 		  saida	= 1.1.3.2.10
   * @param pStrFormato - formato em que deve ficar a conta (deve ser baseado em pontos)
   * @param resposta - número externo da conta sem formatação
   * @return número externo da conta formatado para impressão
   */
  public static String retiraZerosConta(String pStrFormato, String numExtConta) throws Exception {
    String strResposta = formataString(pStrFormato, numExtConta);
    int contador = strResposta.length() - 1;

    if (contador >= 0) {
	    while (strResposta.charAt(contador) == '0' || strResposta.charAt(contador) == '.') {
	        if (strResposta.charAt(contador) == '.') {
	            strResposta = strResposta.substring(0, contador);
	        }
	        contador--;
	    }        
    }
    
    return strResposta;
  }

  /**
    * Formata o campo de acordo com a máscara passada. Caso o formato seja maior do que a string a string formatada 
    * será preenchida com zeros pela esquerda.
    * Exemplo de utilização: 
    * formato: 9-99(9999)
    * string: 345 
    * resultado: 0-00(0345)
    * @param contaVO
    * @return String formatada
    * @throws java.lang.Exception
    */
    public static String formataString(String formato, String string) throws Exception {
      StringBuffer stringFormatada = new StringBuffer();
      
      if (string != null && string.length() > 0) {
        int contadorFormato = 0, contadorString = 0, numeroDeNoves = 0;
        int tamanhoFormato = formato.length();
        int tamanhoString = string.length();
        
        // recupera o número de noves do formato
        for (int i=0; i<tamanhoFormato; i++) {
          if (formato.charAt(i) == '9') {
            numeroDeNoves++;
          }
        }
        
        /*
         * se o número de noves for maior que o tamanho da string, 
         * substitui os noves por zero até que os dois se igualem
         */
        while (numeroDeNoves > tamanhoString) {
            if (formato.charAt(contadorFormato) == '9') {
              stringFormatada.append("0");
              contadorFormato++;
            }
            else {
              stringFormatada.append(formato.charAt(contadorFormato));
              contadorFormato++;
            }             
            numeroDeNoves--;
        }
        
        // substitui os noves por elementos da string, mantendo sempre os outro tokens do formato
        while (contadorString < tamanhoString) {
          
          // anexa o restante da string principal a string formatada quando termina o formato
          if (contadorFormato == tamanhoFormato) {
            stringFormatada.append(string.substring(contadorString));
            break;
          }
          else {
            if (formato.charAt(contadorFormato) == '9') {
              stringFormatada.append(string.charAt(contadorString));
              contadorFormato++;
              contadorString++;
            }
            else {
              stringFormatada.append(formato.charAt(contadorFormato));
              contadorFormato++;
            }
          }
        }
      }
      
      return stringFormatada.toString();
    }
    
    public static String colocaEspacos(Integer numeroEspacos, String string) {
      String espacos = "";
      for (int i=0; i < numeroEspacos.intValue(); i++) {
        espacos = espacos + " ";
      }
      return espacos + string;
    }
    
    /**
    * Retorna um vetor a partir de uma string, sendo cada posição delimitada pelo separador passado.
    * @param  pStrDadoOrig    String a ser explodida
    * @param  pStrSeparador   Separador delimitador das posições do vetor
    * @return Vetor montado a partir da explosão da string
    */
    public static String[] explode(String pStrDadoOrig, String pStrSeparador) {
        String[] lStrRetornaForma = null;
        
        if (pStrDadoOrig != null) {
            StringTokenizer lSttFormaBordero = new StringTokenizer(pStrDadoOrig, pStrSeparador);
            lStrRetornaForma = new String[lSttFormaBordero.countTokens()];
            int i = 0;
            
            while (lSttFormaBordero.hasMoreTokens()) {
                lStrRetornaForma[i++] = lSttFormaBordero.nextToken();
            }
        }
        return lStrRetornaForma;
    }
    
    /**
     * Substitui determinados caracteres especiais
     * @param pDado
     * @return
     */
     public static String substituiAcento(String pDado){
       String lStrDado  = "";
       char   lCaracter = 0;

       for (int i =0; i<pDado.length(); i++){
         lCaracter = pDado.charAt(i);
         switch(lCaracter)
         {
           case 'Á': lStrDado += 'A'; break;
           case 'À': lStrDado += 'A'; break;
           case 'Ã': lStrDado += 'A'; break;
           case 'Â': lStrDado += 'A'; break;
           case 'Ä': lStrDado += 'A'; break;
           case 'á': lStrDado += 'a'; break;
           case 'à': lStrDado += 'a'; break;
           case 'ã': lStrDado += 'a'; break;
           case 'â': lStrDado += 'a'; break;
           case 'ä': lStrDado += 'a'; break;        
           
           case 'É': lStrDado += "E"; break;
           case 'È': lStrDado += "E"; break;
           case 'Ë': lStrDado += "E"; break;
           case 'Ê': lStrDado += "E"; break;
           case 'é': lStrDado += "e"; break;
           case 'è': lStrDado += "e"; break;
           case 'ë': lStrDado += "e"; break;
           case 'ê': lStrDado += "e"; break;
           
           case 'Í': lStrDado += "I"; break;
           case 'Ì': lStrDado += "I"; break;
           case 'Ï': lStrDado += "I"; break;
           case 'Î': lStrDado += "I"; break;
           case 'í': lStrDado += "i"; break;
           case 'ì': lStrDado += "i"; break;
           case 'ï': lStrDado += "i"; break;
           case 'î': lStrDado += "i"; break;
           
           case 'Ó': lStrDado += "O"; break;
           case 'Ò': lStrDado += "O"; break;
           case 'Ö': lStrDado += "O"; break;
           case 'Õ': lStrDado += "O"; break;
           case 'Ô': lStrDado += "O"; break;
           case 'ó': lStrDado += "o"; break;
           case 'ò': lStrDado += "o"; break;
           case 'ö': lStrDado += "o"; break;
           case 'õ': lStrDado += "o"; break;
           case 'ô': lStrDado += "o"; break;
           
           case 'Ú': lStrDado += "U"; break;
           case 'Ù': lStrDado += "U"; break;
           case 'Ü': lStrDado += "U"; break;
           case 'Û': lStrDado += "U"; break;
           case 'ú': lStrDado += "u"; break;
           case 'ù': lStrDado += "u"; break;
           case 'ü': lStrDado += "u"; break;
           case 'û': lStrDado += "u"; break;
           
           case 'Ç': lStrDado += "C"; break;
           case 'ç': lStrDado += "c"; break;
           
           case 'º': lStrDado += 'o'; break;
           case '\n': lStrDado += ' '; break;
         
           default: lStrDado += lCaracter;
         }
       }
       return lStrDado;
     }
     
     /**
      * Substitui um caracter por outro em uma String
      * @param pDado
      * @param charOrigem
      * @param charDestino
      * @return
      */
     public static String substituiCaracter(String pDado, char charOrigem, char charDestino) {
     	char   lCaracter = 0;
     	String stringRetorno = "";
     	for (int i =0; i<pDado.length(); i++){
            lCaracter = pDado.charAt(i);
            if (lCaracter == charOrigem) {
            	lCaracter = charDestino;
            }
            stringRetorno += lCaracter;
     	}
     	return stringRetorno;
     }
    
}
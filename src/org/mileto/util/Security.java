package org.mileto.util;


import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;





/**
 * Classe que define todos os crit�rios de criptografia e seguran�a do aplicativo
 * @author Abrha�o Ribeiro
 * @since 01/04/2010
 *
 */
public class Security {

	/** Definições Secretas do Algoritmo **/	
	private static final short ALGORI_TAMANHO_DEFS 		= 4;
	private static final short ALGORI_MAX_CHARACTERS	= 60;
	private static final short ALGORI_MAX_CHARCRYPTO	= ALGORI_MAX_CHARACTERS + ALGORI_TAMANHO_DEFS;

	private static final short ALGORI_PASSPORT_TOKEN	=	5;				/** Quantidade de caracteres para o Passport Token **/

	private static final int INTERVALO = 700;	
	private static final int DELAY_CHARAT = -5;


	/** Added in 02/04/2017 to use Cipher **/
	//private static final IvParameterSpec ALGORI_IVSPEC = new IvParameterSpec("0102030405060708".getBytes());

	/**
	 * Gera uma chave randomica para a chave
	 * @param key
	 * @return
	 */
	public static String generate(String key) {

		try {
			System.out.println(" ");		 			 

			System.out.println("*** O Jasper View está definindo uma senha criptografada para as chaves passadas ... ");
			//System.out.println("Ocorrencias " + keys.size());
			//for (String key : keys) {

			key = key.trim();

			String pSize = getSecurityCodeTamanho(key);
			String pAleatory = getSecurityCodeAleatorio(Security.ALGORI_MAX_CHARACTERS - key.length());
			String pInverse = getSecurityCodeInverse(key);			

			/** A chave criptografada � a combina��o das tr�s vari�veis String originadas dos algoritmos **/
			final String keyCriptografada = pSize + pAleatory + pInverse; 												

			TimeUnit.MILLISECONDS.sleep(INTERVALO);  

			System.out.println("******************************************************************* ");				
			System.out.println("*** Chave Original      :   " + revealSecurityCode(keyCriptografada));
			System.out.println("*** Chave Criptografada :   " + keyCriptografada);				
			System.out.println("******************************************************************* ");

			TimeUnit.MILLISECONDS.sleep(INTERVALO);  
			//}

			System.out.println("*** Instale as senhas criptografadas apropriadamente no arquivo jasper.properties");
			System.out.println("******************************************************************* ");

			return keyCriptografada;
		} catch (InterruptedException ignored) {
			ignored.printStackTrace();
			return null;
		}
	}

	private static String getSecurityCodeTamanho(String pkey) {
		return lPad(new Integer(pkey.trim().length()*17 - 9).toString(), Security.ALGORI_TAMANHO_DEFS, "0");
	}

	/**
	 * Fornece uma cadeia de caracteres aleatórias, formada por letras minúsculas
	 * @param qtde
	 * @return
	 */
	private static String getSecurityCodeAleatorio(int qtde) {
		String sReturn = "";
		RandomIntGenerator rg = new RandomIntGenerator('a', 'z');			
		for (int i=0; i<qtde; i++) {
			sReturn += (char)rg.draw();
		}
		return sReturn;
	}

	/**
	 * Fornece um nome aleatório, como cadeia numérica de 0 a 9, com quantos algarismos meu cliente quiser
	 * @param howManyCharactersDoYouWant
	 * @return
	 */
	public static String giveMeFuckingName(int howManyCharactersDoYouWant) {
		String sReturn = new String("");
		RandomIntGenerator rg = new RandomIntGenerator('0', '9');			
		for (int i=0; i<howManyCharactersDoYouWant; i++) {
			sReturn += (char)rg.draw();
		}
		return sReturn;
	}	


	private static String getSecurityCodeInverse(String pkey) {
		String sReturn = "";
		for (int i=0; i<pkey.trim().length(); i++) {
			int j = ((int)pkey.charAt(i) - DELAY_CHARAT );
			sReturn = (char)(j) + sReturn;
		}
		return sReturn;
	}

	public static String lPad(String pStrNome, int pNTamanho, String pStrCarac){
		String lStrNome = "";

		int lIntTamanhoNome = pStrNome.length();		    
		int lIntDif = pNTamanho - lIntTamanhoNome;
		for (int i = 0; i < lIntDif; i++)  {
			lStrNome += pStrCarac;
		}

		lStrNome += pStrNome;
		return lStrNome;
	}

	/**
	 * Recebe a chave criptografada e retorna a String original
	 * @param pkeyCript
	 * @return
	 */
	public static String revealSecurityCode(String pkeyCript) throws NumberFormatException {		
		String pkeyDecript = "";
		int psize = (Integer.parseInt(pkeyCript.substring(0,ALGORI_TAMANHO_DEFS)) + 9)/17;		

		pkeyCript = pkeyCript.substring(ALGORI_MAX_CHARCRYPTO - psize, ALGORI_MAX_CHARCRYPTO);
		for (int i=0; i<psize; i++) {
			char ch = (char)(pkeyCript.charAt(i) + DELAY_CHARAT);
			pkeyDecript = ch + pkeyDecript;
		}
		System.out.println("pKeyDecript" + pkeyDecript);		
		return pkeyDecript;
	}



	/* ----------------------------------
	public static void mainCipher(String[] args) {  
		try{
			byte[] plainBytes = "HELLO JCE".getBytes();

			// Generate the key first
			KeyGenerator keyGen = KeyGenerator.getInstance("AES");
			keyGen.init(128);  // Key size
			Key key = keyGen.generateKey();

			// Create Cipher instance and initialize it to encrytion mode
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");  // Transformation of the algorithm
			cipher.init(Cipher.ENCRYPT_MODE, key);
			byte[] cipherBytes = cipher.doFinal(plainBytes);

			System.out.println("ENCRYPTED DATA : "+cipherBytes.toString());

			// Reinitialize the Cipher to decryption mode
			cipher.init(Cipher.DECRYPT_MODE,key, cipher.getParameters());
			byte[] plainBytesDecrypted = cipher.doFinal(cipherBytes);

			System.out.println("DECRUPTED DATA : "+new String(plainBytesDecrypted));
		} catch (Exception x) {  
			x.printStackTrace();  
		}  			
	}  
	**/
	
	/**
	 * Fornece uma chave passaporte token, para utilização das APIs do JasperView
	 * @return
	 */
	public static String giveMePassportToken() {
		return getSecurityCodeAleatorio(5).toUpperCase();
	}


	/**
	 * Gera uma API KEY aleatória
	 * @return
	 */
	public static String giveMePassportApiKey( )  {
		return MD5 (  Security.getSecurityCodeAleatorio(19) );
	}
	
	/**
	 * Determina uma assinatura para o passport token, para utilização das APIs do JasperView
	 * @return
	 */
	public static String giveMyPassportSignature ( String token )  {
		return MD5 (  token );
	}
	
	/**
	 * Verifica se o dono da assinatura é de fato o token
	 * @param signature
	 * @param token
	 * @return
	 */
	public static boolean confereAssinatura (String signature, String token) {
		String assinaturaCriptografada = signature;
		String assinaturaEsperada = MD5(token).toUpperCase();
		
		return assinaturaCriptografada.equals(assinaturaEsperada);
	}
	
	/**
	 * Criptografa uma cadeia de caracteres qualquer com algoritmo MD5
	 * @param cadeia
	 * @return
	 */
	private static String MD5 ( String cadeia ) {
		byte[] thedigest	=	{};
		String hexString = new String("");
		try {
			byte[] bytesOfMessage = cadeia.getBytes(StandardCharsets.UTF_8); // Java 7+ only //Security.getSecurityCodeAleatorio(19).getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			thedigest = md.digest(bytesOfMessage);
			hexString = new String(Hex.encodeHex(thedigest));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return hexString;
		
	}




	/*******************************/

}
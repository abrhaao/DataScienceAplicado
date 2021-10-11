package org.mileto.util;

/**
 * Esta classe � respons�vel por realizar diversos tratamentos de StackTrace provenientes de Exceptions.
 * @author Abrha�o Ribeiro
 */
public class TraceDiscover  {
	
	/**
	 * Monta uma String a ser retornada para um arquivo, a partir de um Stacktrace
	 * @param stacks
	 * @return
	 */
	public static String getMessagesTrace(StackTraceElement[] stacks) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("\n");
		for (int i = 0; i < stacks.length; i++) {
			StackTraceElement stack = stacks[i];			
			buffer.append("-- > Classe  :: "+ stack.getClassName() + " : linha " + stack.getLineNumber()); 
			buffer.append("\n");
			buffer.append("---- M�todo  :: " + stack.getMethodName());
			buffer.append("\n");			
		}
		return buffer.toString();
	}
}
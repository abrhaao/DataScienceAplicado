package org.mileto.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

public class DateAcol extends GregorianCalendar {
	/** Retorna a data no formato dd/MM/yyyy */            
	public static final int DMN_DATA      = 1;
	/** Retorna a hora no formato hh:mm */            
	public static final int DMN_HORA      = 2;
	/** Retorna a hora no formato hh:mm:ss */            
	public static final int DMN_HORA_SEG  = 3;
	/** Retorna a data no formato dd/MM/yyyy hh:mm:ss */            
	public static final int DMN_DATA_COMPL = 4;
	/** Retorna a data no formato dd/MM/yyyy hh:mm */            
	public static final int DMN_DATA_HORA = 5;
	/** Retorna a hora no formato HH:mm a (isto �, exibindo am ou pm)*/  
	public static final int DMN_HORA_AM_PM = 6;
	/** Retorna a data no formato ddMMyyyyhhmmss */
	public static final int DMN_DATA_COMPL_CONCAT = 7;
	/** Retorna a data no formato MM/yyyy */
	public static final int DMN_MES_ANO = 8;
	/** Retorna a data no formato yyyyMM */
	public static final int DMN_ANO_MES_CONCAT = 9;
	/** Retorna a data no formato ddMMyyyy */
	public static final int DMN_DATA_SEM_BARRAS = 10;
	/** Retorna a data no formato yyyy-MM-dd */
	public static final int DMN_DATA_ORDENADA = 99;

	private final int DMNintTotDiasSemana = 7;

	private String strData = new String("");
	private boolean showString = false;

	public DateAcol() {
		super();
	}

	/**
	 * Inicializa o DateAcol passando um objeto java.sql.Date
	 * @param pDateSql Data no formato do banco
	 */
	public DateAcol(java.sql.Date pDateSql){
		java.util.Date ldataUtil = new java.util.Date(pDateSql.getTime());
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		strData = sdf.format(ldataUtil);
		this.setTime(ldataUtil);
	}

	/**
	 * Inicializa o DateAcol passando um objeto java.sql.Date
	 * @param pDateSql Data no formato do banco
	 */
	public DateAcol(Timestamp pTimestamp){
		java.util.Date ldataUtil = new java.util.Date(pTimestamp.getTime());
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		strData = sdf.format(ldataUtil);
		this.setTime(ldataUtil);
	}

	/**
	 * Este construtor recebe tres inteiros representando dia, mes e ano. A partir desses  
	 * dados cria uma instancia do objeto Calendar que encpasular� uma data representada   
	 * pelos dados passados. O formato desejado para apresentar a data para o usu�rio �    
	 * determinado pelo segundo par�metro. 
	 * @param dia
	 * @param mes
	 * @param ano
	 */
	public DateAcol(int dia, int mes, int ano) {
		super();

		ano = ano -1900;

		this.set(Calendar.DAY_OF_MONTH, dia);
		this.set(Calendar.MONTH, mes-1);
		this.set(Calendar.YEAR, ano + 1900);
		//Para n�o ter problemas na diferen�a de dias
		// Por default � incluido a hora corrente
		this.set(Calendar.HOUR_OF_DAY, 0);
		this.set(Calendar.MINUTE, 0);
		this.set(Calendar.SECOND, 0);
		this.set(Calendar.MILLISECOND, 0);
	}

	/**
	 * Este construtor recebe tres inteiros representando dia, mes e ano. A partir desses  
	 * dados cria uma instancia do objeto Calendar que encpasular� uma data representada   
	 * pelos dados passados. O formato desejado para apresentar a data para o usu�rio �    
	 * determinado pelo segundo par�metro. 
	 * @param dia
	 * @param mes
	 * @param ano
	 */
	public DateAcol(String strDia, String strMes, String strAno) {
		super();

		int dia = NumberAcol.nvl(strDia, 0);
		int mes = NumberAcol.nvl(strMes, 0);
		int ano = NumberAcol.nvl(strAno, 0);

		ano = ano -1900;

		this.set(Calendar.DAY_OF_MONTH, dia);
		this.set(Calendar.MONTH, mes-1);
		this.set(Calendar.YEAR, ano + 1900);
		//Para n�o ter problemas na diferen�a de dias
		// Por default � incluido a hora corrente
		this.set(Calendar.HOUR_OF_DAY, 0);
		this.set(Calendar.MINUTE, 0);
		this.set(Calendar.SECOND, 0);
		this.set(Calendar.MILLISECOND, 0);
	}

	/**
	 * 
	 * @param mes
	 * @param ano
	 */
	public DateAcol(int mes, int ano) {
		super();

		ano = ano -1900;

		this.set(Calendar.DAY_OF_MONTH, 1);
		this.set(Calendar.MONTH, mes);
		this.set(Calendar.YEAR, ano + 1900);
		//Para n�o ter problemas na diferen�a de dias
		// Por default � incluido a hora corrente
		this.set(Calendar.HOUR_OF_DAY, 0);
		this.set(Calendar.MINUTE, 0);
		this.set(Calendar.SECOND, 0);
		this.set(Calendar.MILLISECOND, 0);
	}

	/**
	 * Este construtor recebe seis inteiros representando dia, mes, ano, hora, minuto e segundo.           
	 * A partir desses dados cria uma instancia do objeto Calendar que encpasular� uma data representada   
	 * pelos dados passados. O formato desejado para apresentar a data para o usu�rio �                    
	 * determinado pelo segundo par�metro. 
	 * @param dia
	 * @param mes
	 * @param ano
	 * @param hora
	 * @param min
	 * @param seg
	 */
	public DateAcol(int dia, int mes, int ano, int hora, int min, int seg) {
		super();

		ano = ano -1900;

		this.set(Calendar.DAY_OF_MONTH, dia);
		this.set(Calendar.MONTH, mes);
		this.set(Calendar.YEAR, ano + 1900);
		this.set(Calendar.HOUR_OF_DAY, hora);
		this.set(Calendar.MINUTE, min);
		this.set(Calendar.SECOND, seg);
		this.set(Calendar.MILLISECOND, 0);
	}

	/**
	 * Este construtor recebe uma String no formato dd.mm.yyyy e instancia um objeto       
	 * Calendar que encpasular� uma data representada pela String passada. O formato       
	 * desejado para apresentar a data para o usu�rio � determinado pelo segundo par�metro. 
	 * @param pData
	 */
	public DateAcol(String pData) {
		super();

		try{
			String lStrData;

			SimpleDateFormat formatter = new SimpleDateFormat ("dd/MM/yyyy hh:mm:ss");
			if (pData.indexOf("/") == -1){
				SimpleDateFormat form = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
				lStrData = formatter.format(form.parse(pData));
			}else{
				if (pData.indexOf(":") == -1){
					pData = pData + " 00:00:00";
				}
				lStrData = pData;
			}
			/*
          int mes = Integer.parseInt(lStrData.substring(lStrData.indexOf("/")+1, lStrData.lastIndexOf("/")));
          lStrData = lStrData.substring(0, lStrData.indexOf("/")+1) + (mes-1) + lStrData.substring(lStrData.lastIndexOf("/"), lStrData.length());
			 */
			java.util.Date lDatData = formatter.parse(lStrData);

			this.setTime(lDatData);
		}catch (ParseException e){
			System.err.println("Erro Parse DateAcol: "+e.getMessage());
		}
		/*
      StringTokenizer token = new StringTokenizer(lStrData, "/");
      int dia = Integer.parseInt(token.nextToken());
      int mes = Integer.parseInt(token.nextToken());
      int ano = Integer.parseInt(token.nextToken()) - 1900;

      this.set(Calendar.DAY_OF_MONTH, dia);
      this.set(Calendar.MONTH, mes - 1);
      this.set(Calendar.YEAR, ano + 1900);
      //Para n�o ter problemas na diferen�a de dias
      // Por default � incluido a hora corrente
      this.set(Calendar.HOUR_OF_DAY, 0);
      this.set(Calendar.MINUTE, 0);
      this.set(Calendar.SECOND, 0);
      this.set(Calendar.MILLISECOND, 0);
		 */
	}

	public DateAcol(String pStrData, boolean show){
		strData = pStrData;
		showString = show;
	}

	/**
	 * Este construtor recebe uma String no formato dd.mm.yyyy, uma segunda String        
	 * no formato hh.mm.ss e instancia um objeto Calendar que encpasular� uma data         
	 * representada pelas duas Strings passadas. O formato desejado para apresentar a data 
	 * para o usu�rio � determinado pelo segundo par�metro.
	 * @param pData
	 * @param pHora
	 */
	public DateAcol(String pData, String pHora) {
		super();

		StringTokenizer tokenData = new StringTokenizer(pData, "/");
		int dia = Integer.parseInt(tokenData.nextToken());
		int mes = Integer.parseInt(tokenData.nextToken());
		int ano = Integer.parseInt(tokenData.nextToken()) - 1900;

		StringTokenizer tokenHora = new StringTokenizer(pHora, ":");
		int hora= Integer.parseInt(tokenHora.nextToken());
		int min = Integer.parseInt(tokenHora.nextToken());
		int seg = Integer.parseInt(tokenHora.nextToken());

		this.set(Calendar.DAY_OF_MONTH, dia);
		this.set(Calendar.MONTH, mes);
		this.set(Calendar.YEAR, ano + 1900);
		this.set(Calendar.HOUR_OF_DAY, hora);
		this.set(Calendar.MINUTE, min);
		this.set(Calendar.SECOND, seg);
		this.set(Calendar.MILLISECOND, 0);
	}

	/**
	 * 
	 * @param b
	 * @return 
	 */
	public int daysBetween(DateAcol b){
		return toJulian() - b.toJulian();
	}

	/**
	 * 
	 * @param calendarIni
	 * @return 
	 */
	public int getIntervaloEntreDatas(DateAcol calendarIni){
		int i=0;
		DateAcol calendarIniTemp = (DateAcol)calendarIni.clone();
		int lnDiaIniEscolhido = calendarIni.get(DateAcol.DAY_OF_MONTH);

		while( ((calendarIniTemp.get(DateAcol.YEAR)*100) + calendarIniTemp.get(DateAcol.MONTH))<=
				((this.get(DateAcol.YEAR)*100)+ this.get(DateAcol.MONTH))){
			int lnMesProx = calendarIniTemp.get(DateAcol.MONTH) + 1;
			DateAcol lobjMesProx = new DateAcol(1, lnMesProx, calendarIni.get(DateAcol.YEAR));

			if (lnDiaIniEscolhido > lobjMesProx.getActualMaximum(DateAcol.DAY_OF_MONTH)){
				calendarIniTemp.set(DateAcol.DAY_OF_MONTH, lobjMesProx.getActualMaximum(DateAcol.DAY_OF_MONTH));
			}else if (lnDiaIniEscolhido > calendarIni.getActualMaximum(DateAcol.DAY_OF_MONTH)){
				calendarIniTemp.set(DateAcol.DAY_OF_MONTH, lnDiaIniEscolhido);
			}

			if (calendarIniTemp.get(DateAcol.MONTH)==11){
				calendarIniTemp.set(DateAcol.MONTH, 0);
				calendarIniTemp.set(DateAcol.YEAR, calendarIniTemp.get(DateAcol.YEAR) + 1);
			} else {
				calendarIniTemp.set(DateAcol.MONTH, calendarIniTemp.get(DateAcol.MONTH) + 1);
			}

			i++;
		}
		return i;
	}

	/**
	 * Calcula o n�mero de dias entre esta data e uma anterior.
	 * @param calendarIni Uma data anterior a ser calculada
	 * @return O n�mero de dias entre as datas
	 */
	public int getDayRange(DateAcol calendarIni){
		long dataIniMilis = calendarIni.getTime().getTime();
		long dataFimMilis = this.getTime().getTime();

		long lLngRange = dataFimMilis - dataIniMilis;
		return (int)(lLngRange / (1000 * 60 * 60 * 24));
	}

	/**
	 * Calcula o n�mero de dias entre duas datas quaisquer.
	 * @param calendarIni Uma data anterior a ser calculada
	 * @param calendarFim Uma data final a ser calculada
	 * @return O n�mero de dias entre as datas
	 */
	public static int getDayRange(DateAcol calendarIni, DateAcol calendarFim) {
		long dataIniMilis = calendarIni.getTime().getTime();
		long dataFimMilis = calendarFim.getTime().getTime();

		long lLngRange = dataFimMilis - dataIniMilis;
		return (int)(lLngRange / (1000 * 60 * 60 * 24));
	}
	
	/**
	 * Calcula o numero de horas entre duas datas quaisquer.
	 * @param calendarIni Uma data anterior a ser calculada
	 * @param calendarFim Uma data final a ser calculada
	 * @return O numero de horas entre as datas
	 */
	public static int getHoursRange(Date calendarIni, Date calendarFim) {
		long dataIniMilis = calendarIni.getTime();
		long dataFimMilis = calendarFim.getTime();

		long lLngRange = dataFimMilis - dataIniMilis;
		return (int)(lLngRange / (1000 * 60 * 60 ));
	}

	/**
	 * Troca formato de m�s (n�mero)
	 * para data string (ex.: Jan, Fev)
	 * recebendo o formato de DateAcol
	 */
	public String monthToStr(){
		String strMesNum=new String("");
		int lIntMes = this.get(DateAcol.MONTH);
		if(lIntMes == 0){ strMesNum = "Jan";}
		else if(lIntMes == 1){ strMesNum = "Fev";}
		else if(lIntMes == 2){ strMesNum = "Mar";}
		else if(lIntMes == 3){ strMesNum = "Abr";}
		else if(lIntMes == 4){ strMesNum = "Mai";}
		else if(lIntMes == 5){ strMesNum = "Jun";}
		else if(lIntMes == 6){ strMesNum = "Jul";}
		else if(lIntMes == 7){ strMesNum = "Ago";}
		else if(lIntMes == 8){ strMesNum = "Set";}
		else if(lIntMes == 9){ strMesNum = "Out";}
		else if(lIntMes == 10){ strMesNum = "Nov";}
		else if(lIntMes == 11){ strMesNum = "Dez";}
		return strMesNum;
	}

	/**
	 * Troca formato de m�s (n�mero)
	 * para data string (ex.: Jan, Fev)
	 * recebendo o formato de DateAcol
	 */
	public static String monthToStr(int lIntMes){
		String strMesNum=new String("");
		if(lIntMes == 0){ strMesNum = "Jan";}
		else if(lIntMes == 1){ strMesNum = "Fev";}
		else if(lIntMes == 2){ strMesNum = "Mar";}
		else if(lIntMes == 3){ strMesNum = "Abr";}
		else if(lIntMes == 4){ strMesNum = "Mai";}
		else if(lIntMes == 5){ strMesNum = "Jun";}
		else if(lIntMes == 6){ strMesNum = "Jul";}
		else if(lIntMes == 7){ strMesNum = "Ago";}
		else if(lIntMes == 8){ strMesNum = "Set";}
		else if(lIntMes == 9){ strMesNum = "Out";}
		else if(lIntMes == 10){ strMesNum = "Nov";}
		else if(lIntMes == 11){ strMesNum = "Dez";}
		return strMesNum;
	}

	/**
	 * Troca formato de m�s (n�mero)
	 * para data string (ex.: Janeiro, Fevereiro, etc...)
	 * recebendo o formato de DateAcol
	 */
	public String monthToStrExt(){
		String strMesNum=new String("");
		int lIntMes = this.get(DateAcol.MONTH);
		if(lIntMes == 0){ strMesNum = "Janeiro";}
		else if(lIntMes == 1){ strMesNum = "Fevereiro";}
		else if(lIntMes == 2){ strMesNum = "Mar�o";}
		else if(lIntMes == 3){ strMesNum = "Abril";}
		else if(lIntMes == 4){ strMesNum = "Maio";}
		else if(lIntMes == 5){ strMesNum = "Junho";}
		else if(lIntMes == 6){ strMesNum = "Julho";}
		else if(lIntMes == 7){ strMesNum = "Agosto";}
		else if(lIntMes == 8){ strMesNum = "Setembro";}
		else if(lIntMes == 9){ strMesNum = "Outubro";}
		else if(lIntMes == 10){ strMesNum = "Novembro";}
		else if(lIntMes == 11){ strMesNum = "Dezembro";}
		return strMesNum;
	}

	/**
	 * Troca formato de data (mes/ano - mmm/yy ex.:Jan/2000)
	 * para data normal (mes/ano - mm/yyyy ex.: 01/2000)
	 */
	public static String monthToInt(String strMes){
		String strAno = new String("");
		String strMesNum = new String("");
		String sData = new String("");
		// strMes = strMes.substring(0, 3);

		if(strMes.equals("Jan")){ strMesNum = "01";}
		else if(strMes.equals("Fev")){ strMesNum = "02";}
		else if(strMes.equals("Mar")){ strMesNum = "03";}
		else if(strMes.equals("Abr")){ strMesNum = "04";}
		else if(strMes.equals("Mai")){ strMesNum = "05";}
		else if(strMes.equals("Jun")){ strMesNum = "06";}
		else if(strMes.equals("Jul")){ strMesNum = "07";}
		else if(strMes.equals("Ago")){ strMesNum = "08";}
		else if(strMes.equals("Set")){ strMesNum = "09";}
		else if(strMes.equals("Out")){ strMesNum = "10";}
		else if(strMes.equals("Nov")){ strMesNum = "11";}
		else if(strMes.equals("Dez")){ strMesNum = "12";}

		return strMesNum;
	}

	public Object clone(){
		return super.clone();
	}

	/**
	 *
	 * @return
	 */
	private int toJulian()
	{  int jy = this.get(DateAcol.YEAR);
	if (this.get(DateAcol.YEAR) < 0) jy++;
	int jm = this.get(DateAcol.MONTH);
	if (this.get(DateAcol.MONTH) > 2) jm++;
	else
	{  jy--;
	jm += 13;
	}
	int jul = (int) (java.lang.Math.floor(365.25 * jy)
			+ java.lang.Math.floor(30.6001*jm) + this.get(DateAcol.DAY_OF_MONTH) + 1720995.0);

	int IGREG = 15 + 31*(10+12*1582);
	// Gregorian Calendar adopted Oct. 15, 1582

	if (this.get(DateAcol.DAY_OF_MONTH) + 31 * (this.get(DateAcol.MONTH) + 12 * this.get(DateAcol.YEAR)) >= IGREG)
		// change over to Gregorian calendar
	{  int ja = (int)(0.01 * jy);
	jul += 2 - ja + (int)(0.25 * ja);
	}
	return jul;
	}

	/**
	 * Recupera o dia da semana
	 * @return O dia da semana
	 */
	public int getDayOfWeek(){
		return this.get(DateAcol.DAY_OF_WEEK);
	}

	/**
	 * Recupera o dia da semana por extenso
	 * @return O dia da semana por extenso
	 */
	public String getNameDayOfWeek(){
		int lIntDayOfWeek = this.get(DateAcol.DAY_OF_WEEK);
		String lStrDayOfWeek = "";
		switch (lIntDayOfWeek) {
		case DateAcol.SUNDAY:     lStrDayOfWeek = "Domingo";
		break;
		case DateAcol.MONDAY:     lStrDayOfWeek = "Segunda";
		break;
		case DateAcol.TUESDAY:    lStrDayOfWeek = "Ter�a";
		break;
		case DateAcol.WEDNESDAY:  lStrDayOfWeek = "Quarta";
		break;
		case DateAcol.THURSDAY:   lStrDayOfWeek = "Quinta";
		break;
		case DateAcol.FRIDAY:     lStrDayOfWeek = "Sexta";
		break;
		case DateAcol.SATURDAY:   lStrDayOfWeek = "S�bado";
		break;
		}

		return lStrDayOfWeek;
	}

	/**
	 * Recupera o dia no m�s corrente
	 * @return O dia no m�s corrente
	 */
	public int getDayOfMonth(){
		return this.get(DateAcol.DAY_OF_MONTH);
	}

	/**
	 * Recupera o m�s no ano corrente
	 * @return O m�s no ano corrente
	 */
	public int getMonth(){
		return this.get(DateAcol.MONTH) + 1;
	}

	/**
	 * Recupera o ano corrente
	 * @return O ano corrente
	 */
	public int getYear(){
		return this.get(DateAcol.YEAR);
	}


	/**
	 * Recupera a hora corrente
	 */
	public int getHour(){
		return this.get(DateAcol.HOUR);
	}


	/**
	 * Recupera o minuto corrente
	 */
	public int getMinute(){
		return this.get(DateAcol.MINUTE);
	}


	/**
	 * Recupera o segundo corrente
	 */
	public int getSecond(){
		return this.get(DateAcol.SECOND);
	}

	/**
	 * Transforma o DateAcol em um objeto do tipo TimeStamp
	 * @return Timestamp a partir do DateAcol
	 */
	public Timestamp toTimeStamp(){
		long dataMilis = this.getTime().getTime();

		return new Timestamp(dataMilis);
	}

	public String getStrDate(){
		SimpleDateFormat formatter = new SimpleDateFormat ("dd/MM/yyyy");
		return formatter.format(this.getTime());

	}

	public String getStrTime(){
		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm");
		return formatter.format(this.getTime());
	}

	public String getStrTimeSeconds(){
		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
		return formatter.format(this.getTime());
	}    

	/**
	 *
	 * @param pIntTipo 
	 * @return
	 */
	public String getDate(int pIntTipo){
		String lStrMascara = "";
		switch(pIntTipo){
		case DMN_DATA:
			lStrMascara = "dd/MM/yyyy";
			break;
		case DMN_HORA:
			lStrMascara = "hh:mm";
			break;
		case DMN_HORA_SEG:
			lStrMascara = "hh:mm:ss";
			break;
		case DMN_DATA_COMPL:
			lStrMascara = "dd/MM/yyyy HH:mm:ss";
			break;
		case DMN_DATA_HORA:
			lStrMascara = "dd/MM/yyyy hh:mm";
			break;
		case DMN_HORA_AM_PM:
			lStrMascara = "HH:mm a";
			break;
		case DMN_DATA_COMPL_CONCAT:
			lStrMascara = "ddMMyyyyhhmmss";
			break;
		case DMN_MES_ANO:
			lStrMascara = "MM/yyyy";
			break;
		case DMN_ANO_MES_CONCAT:
			lStrMascara = "yyyyMM";
			break;
		case DMN_DATA_SEM_BARRAS:
			lStrMascara = "ddMMyyyy";
			break;
		case DMN_DATA_ORDENADA:
			lStrMascara = "yyyy-MM-dd";
			break;			
		}

		SimpleDateFormat formatter = new SimpleDateFormat(lStrMascara);

		if (this != null) {
			return formatter.format(this.getTime());
		}else{
			return null;
		}
	}

	/**
	 *
	 * @return
	 */
	public String toString() {
		if(! showString) {
			return this.getDate(DateAcol.DMN_DATA_COMPL);
		} else {
			return strData;
		}
	}

	/**
	 * Retorna a data atual + pIntNumDias, esse m�todo verifica se a soma
	 * cai no Final de Semana e adiciona dias a ela, para que so seja v�lido os
	 * dias �teis da semana.
	 * @param pIntNumDias
	 * @return DateAcol
	 */
	public DateAcol getBusinessPlus(int pIntNumDias) {
		DateAcol date = (DateAcol)this.clone();

		for (int i=1; i<=pIntNumDias; i++){
			date.set(DateAcol.DAY_OF_YEAR, date.get(DateAcol.DAY_OF_YEAR)+1);
			if (date.getDayOfWeek() == DateAcol.SUNDAY){  // Se for igual a domingo
				date.set(DateAcol.DAY_OF_YEAR, date.get(DateAcol.DAY_OF_YEAR)+1);
			} else if (date.getDayOfWeek() == DateAcol.SATURDAY){   // Se for igual a s�bado
				date.set(DateAcol.DAY_OF_YEAR, date.get(DateAcol.DAY_OF_YEAR)+2);
			}
		}

		return date;
	}

	/**
	 * Adianta a data no n�mero de dias passado por par�metro.
	 * @param pIntNumDias N�mero de dias a ser adiantado
	 */
	public void getDatePlus(int pIntNumDias) {
		this.set(DateAcol.DAY_OF_YEAR, this.get(DateAcol.DAY_OF_YEAR)+pIntNumDias);
	}

	/**
	 * Retrocede a data no n�mero de dias passado por par�metro.
	 * @param pIntNumDias N�mero de dias a ser retrocedido
	 */
	public void getDateMinus(int pIntNumDias) {
		this.set(DateAcol.DAY_OF_YEAR, this.get(DateAcol.DAY_OF_YEAR)-pIntNumDias);
	}

	/**
	 * Adianta a data no n�mero de meses passado por par�metro.
	 * @param pIntNumMeses N�mero de meses a ser adiantado
	 */
	public void getMonthPlus(int pIntNumMeses) {
		this.set(DateAcol.MONTH, this.get(DateAcol.MONTH)+pIntNumMeses);
	}

	/**
	 * Retorna o primeiro dia da semana da data corrente.
	 * @return Primeiro dia da semana
	 */
	public DateAcol getFirstDateOfWeek() {
		DateAcol firstDate = (DateAcol)this.clone();

		int diff = DateAcol.SUNDAY - firstDate.get(DateAcol.DAY_OF_WEEK) - 1;

		firstDate.add( DateAcol.DAY_OF_YEAR, diff );

		firstDate.set(DateAcol.HOUR, 12);
		firstDate.set(DateAcol.MINUTE, 0);

		return firstDate;
	}

	/**
	 * Retorna o ultimo dia da semana da data corrente.
	 * @return  Ultimo dia da semana
	 */
	public DateAcol getLastDateOfWeek() {
		DateAcol lastDate = (DateAcol)this.clone();

		// lastDate.set(this.DAY_OF_WEEK, this.get(this.SATURDAY));
		int diff = DateAcol.SATURDAY - lastDate.get(DateAcol.DAY_OF_WEEK) - 1;
		if ( diff < 0 ){
			diff = 6 + diff;
		}
		lastDate.add( DateAcol.DAY_OF_YEAR, diff );

		lastDate.set(DateAcol.HOUR, 12);
		lastDate.set(DateAcol.MINUTE, 0);

		return lastDate;
	}

	public DateAcol getNextSaturday() {
		DateAcol data = (DateAcol)this.clone();
		data.set(DateAcol.DAY_OF_WEEK, this.get(DateAcol.SATURDAY));
		data.set(DateAcol.HOUR, 12);
		data.set(DateAcol.MINUTE, 0);

		return data;
	}

	public int compareTo(DateAcol when) {
		if (this.before(when)) {
			return -1;
		} 
		if (this.after(when)) {
			return 1;
		}
		return 0;
	}

	public static int diasNoMes(int m, int ano) {    
		switch (m) {      
		case 1:      
		case 3:      
		case 5:      
		case 7:      
		case 8:      
		case 10:      
		case 12: return(31);       
		case 4:      
		case 6:      
		case 9:
		case 11: return(30);       
		default: 
			if (anoBissexto(ano))
				return(29);               
			else return(28);   
		}  
	} 

	public static boolean anoBissexto(int a) {    
		if ((a % 4) != 0)       
			return false;    
		else if ((a % 100) != 0)       
			return true;    
		else if ((a % 400) != 0)       
			return false;    
		else       
			return true;  
	}

	public static Integer qtdDeMesesNoIntervalo(String anoMesInicial, String anoMesFinal){
		String anoInicial = anoMesInicial.substring(0, 4);
		String mesInicial = anoMesInicial.substring(4, 6);
		String anoFinal = anoMesFinal.substring(0, 4);
		String mesFinal = anoMesFinal.substring(4, 6);
		int diferencaEntreAnos = Integer.parseInt(anoFinal) - Integer.parseInt(anoInicial);
		int diferencaEntreMeses = Integer.parseInt(mesFinal) - Integer.parseInt(mesInicial);
		return new Integer(diferencaEntreAnos * 12 + diferencaEntreMeses + 1);
	}  

	/**
	 * Retorna dias anteriores a data passada.
	 * @param data e quantidade de dias anteriores
	 */
	 public static String getDiasAnteriores(String data, int qtdDias){

		StringTokenizer token = new StringTokenizer(data, "/");
		String dia = token.nextToken();
		String mes = token.nextToken();
		String ano = token.nextToken();

		DateAcol dataMenos = new DateAcol(dia, mes, ano);
		dataMenos.getDateMinus(qtdDias);
		return dataMenos.getStrDate();
	 }    

	 /**
	  * Retorna uma cole��o de per�odos no intervalo de datas, separado por ano
	  */
	 public static List getPeriodosEntreDatas(DateAcol dataInicial, DateAcol dataFinal) {
		 int qtdAnos = dataFinal.getYear() - dataInicial.getYear();
		 List<DateAcol[]> listaAnos = new ArrayList();
		 if (qtdAnos >= 0) {
			 for (int y=0; y<=qtdAnos; y++) {
				 DateAcol[] periodos = {new DateAcol("01/01/" + (new Integer(dataInicial.getYear() + y)).toString()), 
						 new DateAcol("31/12/" + (new Integer(dataInicial.getYear() + y)).toString())};
				 if (y==0) {
					 periodos[0] = dataInicial;				  
				 }
				 if (y==qtdAnos) {
					 periodos[1] = dataFinal;
				 }
				 listaAnos.add(periodos); 
			 }
		 }
		 return listaAnos;
	 }

	 /**
	  * SimpleDateFormat que utiliza o formato dd/MM/yyyy. 
	  */
	 private static SimpleDateFormat sdf_padrao = newSdfPadrao();

	 private static SimpleDateFormat newSdfPadrao()
	 {
		 SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		 sdf.setLenient(false);
		 return sdf;
	 }

	 /**
	  * M�todo que retorna se uma String contendo uma data no formato
	  * dd/mm/yyyy � uma data v�lida.
	  * 
	  * @param data String contendo a data a ser validada.
	  * @return True caso a data seja v�lida e false caso contr�rio.
	  */
	 public static boolean isValidDate(String data)
	 {
		 boolean ok = false;
		 try 
		 {
			 String ano = data.substring(data.lastIndexOf("/")+1);
			 if (ano.length() == 4)
			 {
				 Integer.parseInt(ano);
				 sdf_padrao.parse(data);
				 ok = true;
			 }
		 }
		 catch (ParseException e){}
		 catch (NumberFormatException e){}

		 return ok;
	 }


	 public static String formatData (Object objData, Locale locale) {
		 
		 String dataFormatada;

		 /** Caso a data esteja como String no formato Protheus yyyymmdd **/
		 try {
			 if (objData instanceof String) {
				 objData = (Date)(new SimpleDateFormat("yyyyMMdd")).parse(objData.toString()); 
			 }
		 } catch (ParseException e) {
			 e.printStackTrace();
		 }
		 
		 if (objData instanceof java.util.Date) { 
			 dataFormatada = DateFormat.getDateInstance(DateFormat.MEDIUM, locale).format(objData);
		 } else {			
			 dataFormatada = null;
		 }
		 return dataFormatada;
	 }
	 
	 public static String getMes(Object objData, Locale locale) {
		 
		 String mes;
		 
		 /** Caso a data esteja como String no formato Protheus yyyymmdd **/
		 try {
			 if (objData instanceof String) {
				 objData = (Date)(new SimpleDateFormat("yyyyMMdd")).parse(objData.toString()); 
			 }
		 } catch (ParseException e) {
			 e.printStackTrace();
		 }
		 
		 if (objData instanceof java.util.Date) { 
			 mes = new DateAcol((Timestamp)objData).monthToStrExt();
		 } else {			
			 mes = null;
		 }
		 return mes;
	 }
}

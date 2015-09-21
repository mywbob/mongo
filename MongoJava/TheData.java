package com.udel.mywbob;

public class TheData {
	private String dataPattern;//the dp for the queried cols
	private String allNumbers;//
	private String[] allStrings;//


	
	public TheData (String n, String[] s, String dp) {
		this.dataPattern = dp;
		this.allNumbers = n;
		this.allStrings = s;
	}
	
	public String getDp() {
		return dataPattern;
	}
	
	public String getNum() {
		return allNumbers;
	}
	
	public String[] getStr() {
		return allStrings;
	}
	
	public int colsAreNumbers() {
		int temp =0;;
		for (int i=0;i<dataPattern.length();i++) {
			if (dataPattern.charAt(i) != 's' && dataPattern.charAt(i) != 't')
				temp++;
		}
		return temp;
	}
	
	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();
		for (int j=0;j<allStrings.length;j++) {
			res.append(allStrings[j]);
			res.append(" ");		
		}
		return res.toString();
	}
	
	
}

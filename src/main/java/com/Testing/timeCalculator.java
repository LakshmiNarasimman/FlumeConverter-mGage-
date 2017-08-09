package com.Testing;

public class timeCalculator {
public static void main(String[] args) {
	String startTime="1500469307741";
	String endTime="1500469681995";
	long timeTaken=Long.parseLong(endTime)-Long.parseLong(startTime);
	System.out.println(timeTaken/(1000*60));
	
	
}
}

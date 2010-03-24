package azkaban.examples.test;

import com.google.common.collect.HashMultimap;

public class HelloWorld {
	public static final String INPUT_ENV = "INPUT_ENV";
	public static void main(String[] args) {
		System.out.println("Environment Variable: " + System.getenv("INPUT_ENV"));
		
		for (String arg: args) {
			System.out.println("Args " + arg);
		}
		
		System.out.println("HELLO WORLD!!!");
	
		HashMultimap<String, Integer> testclassLoading = HashMultimap.<String, Integer>create();
	}
}

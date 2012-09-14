package org.weso.initialize;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class InitializeReducerTest {

	private InitializeReducer initializeReducer;
	
	@Before
	public void testStart(){
		this.initializeReducer = new InitializeReducer();
	}
	
	@Test
	public void filterValuesSeveralUsers(){
		Set<Text> inputSet = new HashSet<Text>();
		inputSet.add(new Text("john"));
		inputSet.add(new Text("mark"));
		
		Set<String> followers = new HashSet<String>();
		followers.add("john");
		followers.add("mark");
		
		initializeReducer.filterValues(inputSet);
		
		assertEquals(followers,initializeReducer.followers);
		
	}
}

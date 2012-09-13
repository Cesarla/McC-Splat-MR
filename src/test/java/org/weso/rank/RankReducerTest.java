package org.weso.rank;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class RankReducerTest {
	
	private RankReducer rankReducer;

	@Before
	public void testStart() {
		this.rankReducer = new RankReducer();
	}
	
	@Test
	public void getUserNameSuccess() {
		assertEquals("john",
				rankReducer.getUserName("john#50.0:ANTISYSTEM#50.0:UNDEFINED"));
	}
	
	@Test
	public void loadUsersEmptyFollowees(){
		Set<Text> input = new HashSet<Text>();
		
		rankReducer.loadUsers(input);
		
		assertEquals(Collections.EMPTY_SET,rankReducer.followees);
	}
	
	@Test
	public void loadUsersEmptyFollowers(){
		Set<Text> input = new HashSet<Text>();
		
		rankReducer.loadUsers(input);
		
		assertEquals(Collections.EMPTY_SET,rankReducer.followers);
		
	}
	
	@Test
	public void loadUsersEmptySize(){
		Set<Text> input = new HashSet<Text>();
		
		rankReducer.loadUsers(input);
		
		assertEquals(rankReducer.followeesSize,0);
	}
	
	@Test
	public void loadUsersMixedFollowees(){
		Set<Text> input = new HashSet<Text>();
		input.add(new Text("0pepe#100.0:ANTISYSTEM@V"));
		input.add(new Text("1juan#100.0:ANTISYSTEM@V"));
		
		rankReducer.loadUsers(input);
		
		Set<String> expectedFollowees = new HashSet<String>();
		expectedFollowees.add("pepe#100.0:ANTISYSTEM@V");
		
		assertEquals(expectedFollowees,rankReducer.followees);
	}
	
	@Test
	public void loadUsersMixedFollowers(){
		Set<Text> input = new HashSet<Text>();
		input.add(new Text("0pepe#100.0:ANTISYSTEM@V"));
		input.add(new Text("1juan#100.0:ANTISYSTEM@V"));
		
		rankReducer.loadUsers(input);
		
		Set<String> expectedFollowers = new HashSet<String>();
		expectedFollowers.add("juan#100.0:ANTISYSTEM@V");
		
		assertEquals(expectedFollowers,rankReducer.followers);
		
	}
	
	@Test
	public void loadUsersMixedSize(){
		Set<Text> input = new HashSet<Text>();
		input.add(new Text("0pepe#100.0:ANTISYSTEM@V"));
		input.add(new Text("1juan#100.0:ANTISYSTEM@V"));
		
		rankReducer.loadUsers(input);
		
		assertEquals(rankReducer.followeesSize,1);
	}
	
	@Test
	public void getValuesVerifiedUser(){
		Map<String,Double> output = new HashMap<String,Double>();
		output.put("ANTISYSTEM@V", new Double("50.0"));
		
		rankReducer.currentUser = "john";
		
		assertEquals(output,
				rankReducer.getValues("john#50.0:ANTISYSTEM@V"));
	}
	
	
	@Test
	public void getValuesUnverifiedUser(){
		Map<String,Double> output = new HashMap<String,Double>();
		output.put("ANTISYSTEM", new Double("50.0"));
		output.put("UNDEFINED", new Double("50.0"));
		
		rankReducer.currentUser = "john";
		
		assertEquals(output,
				rankReducer.getValues("paul#50.0:ANTISYSTEM#50.0:UNDEFINED"));
	}
	
	@Test
	public void getValuesVerifiedFollower(){
		Map<String,Double> output = new HashMap<String,Double>();
		output.put("ANTISYSTEM", new Double("100.0"));
		
		rankReducer.currentUser = "john";
		
		assertEquals(output,
				rankReducer.getValues("doe#100.0:ANTISYSTEM@V"));
	}
	
	@Test
	public void loadValuesSingleUser(){
		Map<String,Double> output = new HashMap<String,Double>();
		output.put("ANTISYSTEM", new Double("50.0"));
		output.put("UNDEFINED", new Double("50.0"));
		
		rankReducer.currentUser = "john";
		
		rankReducer.followees = new HashSet<String> ();
		rankReducer.followees.add("paul#50.0:ANTISYSTEM#50.0:UNDEFINED");
		
		assertEquals(output,
				rankReducer.loadValues());
	}
	
	@Test
	public void loadValuesSeveralUsers(){
		Map<String,Double> output = new HashMap<String,Double>();
		output.put("ANTISYSTEM", new Double("110.0"));
		output.put("UNDEFINED", new Double("90.0"));
		
		rankReducer.currentUser = "john";
		
		rankReducer.followees = new HashSet<String> ();
		rankReducer.followees.add("fred#50.0:ANTISYSTEM#50.0:UNDEFINED");
		rankReducer.followees.add("paul#60.0:ANTISYSTEM#40.0:UNDEFINED");
		
		assertEquals(output,
				rankReducer.loadValues());
	}
	
	@Test
	public void loadValuesVerifiedUser(){
		Map<String,Double> output = new HashMap<String,Double>();
		output.put("ANTISYSTEM@V", new Double("100.0"));
		
		rankReducer.currentUser = "john";
		
		rankReducer.followees = new HashSet<String> ();
		rankReducer.followees.add("doe#50.0:ANTISYSTEM#50.0:UNDEFINED");
		rankReducer.followees.add("john#100.0:ANTISYSTEM@V");
		assertEquals(output,
				rankReducer.loadValues());
	}
	
	@Test
	public void calculateRankNoFollowers(){
		
		rankReducer.currentUser = "john";
		rankReducer.followees = new HashSet<String> ();
		
		rankReducer.calculateRank();
		
		assertEquals(new Text("john#100.0000:UNDEFINED"),
				rankReducer.result);
	}
	
	@Test
	public void calculateRankOneFollower(){
		
		rankReducer.currentUser = "john";
		rankReducer.followees = new HashSet<String> ();
		rankReducer.followees.add("paul#50.0:ANTISYSTEM#50.0:UNDEFINED");
		rankReducer.followeesSize = rankReducer.followees.size();
		
		rankReducer.calculateRank();
		
		assertEquals(new Text("john#50.0:UNDEFINED#50.0:ANTISYSTEM"),
				rankReducer.result);
	}
	
	
	@Test
	public void calculateRankSeveralUser(){
		
		rankReducer.currentUser = "john";
		rankReducer.followees = new HashSet<String> ();
		rankReducer.followees.add("paul#50.0:ANTISYSTEM#50.0:UNDEFINED");
		rankReducer.followees.add("paul#60.0:ANTISYSTEM#40.0:UNDEFINED");
		rankReducer.followeesSize = rankReducer.followees.size();
		
		rankReducer.calculateRank();
		
		assertEquals(new Text("john#45.0:UNDEFINED#55.0:ANTISYSTEM"),
				rankReducer.result);
	}
	
}

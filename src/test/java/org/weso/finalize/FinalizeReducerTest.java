package org.weso.finalize;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class FinalizeReducerTest {
	
	private FinalizeReducer finalizeReducer;
	
	@Before
	public void startTest(){
		this.finalizeReducer = new FinalizeReducer();
		
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("ANTISYSTEM", new Double(55));
		map.put("UNDEFINED", new Double(35));
		map.put("FOO", new Double(10));
		this.finalizeReducer.sinkProperties = map;
	}
	
	@Test
	public void processValuesEmtpyFollowes(){
		assertEquals(Collections.EMPTY_MAP, finalizeReducer.processValues(Collections.<Text> emptySet()));
	}
	
	@Test
	public void processValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put("ANTISYSTEM", new Double(100));
		
		Set<Text> input = new HashSet<Text>();
		input.add(new Text("100.00:ANTISYSTEM"));
		
		assertEquals(map,finalizeReducer.processValues(input));
	}
	
	@Test
	public void processPlainVanillaEmpty(){
		assertEquals(new Text("UNDEFINED"),finalizeReducer.processPlainVanilla(Collections.<String,Double>emptyMap()));
	}
	
	@Test
	public void processPlainVanillaWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put("UNDEFINED", new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		assertEquals(new Text("ANTISYSTEM"),finalizeReducer.processPlainVanilla(map));
	}
	
	@Test
	public void processSinkAbsoluteEmpty(){
		assertEquals(new Text("UNDEFINED"),finalizeReducer.processSinkAbsolute(Collections.<String,Double>emptyMap()));
	}
	
	@Test
	public void processSinkAbsoluteWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put("UNDEFINED", new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		assertEquals(new Text("ANTISYSTEM"),finalizeReducer.processSinkAbsolute(map));
	}
	
	@Test
	public void processSinkRelativeEmpty(){
		assertEquals(new Text("UNDEFINED"),finalizeReducer.processSinkRelative(Collections.<String,Double>emptyMap()));
	}
	
	@Test
	public void processSinkRelativeWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put("UNDEFINED", new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		assertEquals(new Text("ANTISYSTEM"),finalizeReducer.processSinkRelative(map));
	}
	
	@Test
	public void processSinkRelativeWithNoValidValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put("UNDEFINED", new Double(54));
		map.put("ANTISYSTEM", new Double(15));
		map.put("FOO", new Double(9));
		
		assertEquals(new Text("UNDEFINED"),finalizeReducer.processSinkRelative(map));
	}
	
	@Test
	public void processPercentileEmpty(){
		assertEquals(new Text("UNDEFINED"),finalizeReducer.processPercentile(Collections.<String,Double>emptyMap()));
	}
	
	@Test
	public void processPercentileWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put("UNDEFINED", new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		finalizeReducer.percentile = 60;
		
		assertEquals(new Text("ANTISYSTEM"),finalizeReducer.processPercentile(map));
	}
	
	@Test
	public void getPropertyNameVerified(){
		assertEquals("ANTISYSTEM", finalizeReducer.getPropertyName("ANTISYSTEM@V"));
	}
	
	@Test
	public void getPropertyNameUnverified(){
		assertEquals("ANTISYSTEM", finalizeReducer.getPropertyName("ANTISYSTEM"));
	}
}

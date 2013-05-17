package es.weso.finalize;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import es.weso.utils.Format;

import org.apache.hadoop.mapreduce.Reducer.Context;

import static org.mockito.Mockito.*;


public class FinalizeReducerTest {
	
	private FinalizeReducer finalizeReducer;
	
	@Before
	public void startTest(){
		this.finalizeReducer = new FinalizeReducer();
		
		Map<String, Double> map = new HashMap<String, Double>();
		map.put("ANTISYSTEM", new Double(55));
		map.put(Format.UNDEFINED, new Double(35));
		map.put("FOO", new Double(10));
		this.finalizeReducer.sinkProperties = map;
		
		Context context = mock(Context.class);
		doNothing().when(context).progress();
		
		this.finalizeReducer.context = context; 
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
		finalizeReducer.processPlainVanilla(Collections.<String,Double>emptyMap());
		assertEquals(new Text(Format.UNDEFINED),finalizeReducer.resultValue);
	}
	
	@Test
	public void processPlainVanillaWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put(Format.UNDEFINED, new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		finalizeReducer.processPlainVanilla(map);
		assertEquals(new Text("ANTISYSTEM"),finalizeReducer.resultValue);
	}
	
	@Test
	public void processSinkAbsoluteEmpty(){
		
		finalizeReducer.processSinkAbsolute(Collections.<String,Double>emptyMap());
		assertEquals(new Text(Format.UNDEFINED),finalizeReducer.resultValue);
	}
	
	@Test
	public void processSinkAbsoluteWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put(Format.UNDEFINED, new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		finalizeReducer.processSinkAbsolute(map);
		assertEquals(new Text("ANTISYSTEM"),finalizeReducer.resultValue);
	}
	
	@Test
	public void processSinkRelativeEmpty(){
		finalizeReducer.processSinkRelative(Collections.<String,Double>emptyMap());
		assertEquals(new Text(Format.UNDEFINED),finalizeReducer.resultValue);
	}
	
	@Test
	public void processSinkRelativeWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put(Format.UNDEFINED, new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		finalizeReducer.processSinkRelative(map);
		assertEquals(new Text("ANTISYSTEM"),finalizeReducer.resultValue);
	}
	
	@Test
	public void processSinkRelativeWithNoValidValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put(Format.UNDEFINED, new Double(54));
		map.put("ANTISYSTEM", new Double(15));
		map.put("FOO", new Double(9));
		
		finalizeReducer.processSinkRelative(map);
		assertEquals(new Text(Format.UNDEFINED),finalizeReducer.resultValue);
	}
	
	@Test
	public void processPercentileEmpty(){
		finalizeReducer.processPercentile(Collections.<String,Double>emptyMap());
		assertEquals(new Text(Format.UNDEFINED),finalizeReducer.resultValue);
	}
	
	@Test
	public void processPercentileWithValues(){
		Map<String,Double> map = new HashMap<String, Double>();
		map.put(Format.UNDEFINED, new Double(20));
		map.put("ANTISYSTEM", new Double(80));
		
		finalizeReducer.percentile = 60;
		
		finalizeReducer.processPercentile(map);
		assertEquals(new Text("ANTISYSTEM"), finalizeReducer.resultValue);
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

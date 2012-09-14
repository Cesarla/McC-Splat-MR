package org.weso.initialize;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class InitializeMapperTest {
	
	private InitializeMapper initializeMapper;
	
	@Before
	public void testStart(){
		this.initializeMapper = new InitializeMapper();
		this.initializeMapper.verifiedData = new HashMap<String,String>();
	}
	
	@Test
	public void getPropertyUndefined(){
		assertEquals("UNDEFINED",initializeMapper.getProperty("john"));
	}
	
	@Test
	public void getPropertyDefined(){
		initializeMapper.verifiedData.put("john", "ANTISYSTEM");
		assertEquals("ANTISYSTEM",initializeMapper.getProperty("john"));
	}
	
	@Test
	public void generateUserUndefined(){
		assertEquals(new Text("john#100.0000:UNDEFINED"),initializeMapper.generateUser("john"));
	}
	
	@Test
	public void generateUserDefined(){
		initializeMapper.verifiedData.put("john", "ANTISYSTEM");
		assertEquals(new Text("john#100.0000:ANTISYSTEM@V"),initializeMapper.generateUser("john"));
	}
}

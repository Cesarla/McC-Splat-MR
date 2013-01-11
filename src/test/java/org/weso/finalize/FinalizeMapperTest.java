package org.weso.finalize;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class FinalizeMapperTest {
	
	private FinalizeMapper finalizeMapper;
	
	@Before
	public void startTest(){
		this.finalizeMapper = new FinalizeMapper();
	}
	
	@Test
	public void getProperties(){
		String[] properties = new String[]{"50.000:UNDEF","50.000:ANTISYSTEM"};
		assertEquals(properties,finalizeMapper.getProperties("50.000:UNDEF#50.000:ANTISYSTEM"));
	}
	
	@Test
	public void getUserName(){
		assertEquals("john",finalizeMapper.getUserName("john#100.000:UNDEF"));
	}
}

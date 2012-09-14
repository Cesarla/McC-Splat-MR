package org.weso.finalize;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

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
		Set<String> set = new HashSet<String>();
		set.add("50.000:UNDEF");
		set.add("50.000:ANTISYSTEM");
		
		assertEquals(set,finalizeMapper.getProperties("john#50.000:UNDEF#50.000:ANTISYSTEM"));
	}
	
	@Test
	public void getUserName(){
		assertEquals("john",finalizeMapper.getUserName("john#100.000:UNDEF"));
	}
}

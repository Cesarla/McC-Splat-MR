package org.weso.rank;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class RankMapperTest {

	private RankMapper rankMapper;

	@Before
	public void testStart() {
		this.rankMapper = new RankMapper();
	}

	@Test
	public void hasValidLengthSuccessValidInput() {
		String [] input = new String[]{"0","1"};
		assertEquals(true, rankMapper.hasValidLength(input));
	}

	@Test
	public void hasValidLengthSuccessInValidInput() {
		String [] input = new String[]{"0"};
		assertEquals(false, rankMapper.hasValidLength(input));
	}
	
	@Test
	public void getUserNameSuccess() {
		assertEquals("john",
				rankMapper.getUserName("john#ANTISYSTEM:50.0#UNDEFINED:50.0"));
	}

	@Test
	public void isVerifiedSucess1() {
		assertEquals(true, rankMapper.isVerified("aux@V"));
	}

	@Test
	public void isVerifiedSuccess2() {
		assertEquals(false, rankMapper.isVerified("aux"));
	}
}

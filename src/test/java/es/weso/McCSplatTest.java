package es.weso;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import es.weso.utils.Mode;

/**
 * Unit test for simple App.
 */
public class McCSplatTest{

	private McCSplat mcCSplat;

	@Before
	public void beforeTest() {
		this.mcCSplat = McCSplat.getInstance();
	}
	
	@After
	public void afterTest(){
		McCSplat.MCCSPLAT_INSTANCE=null;
	}
	
	@Test
	public void getModeUndefined() throws CmdLineException{
		assertEquals(Mode.PLAIN_VANILLA, mcCSplat.getMode());
	}
	
	@Test
	public void getModeDefined() throws CmdLineException{
		mcCSplat.mode = Mode.SINK_ABSOLUTE;
		assertEquals(Mode.SINK_ABSOLUTE, mcCSplat.getMode());	
	}
}

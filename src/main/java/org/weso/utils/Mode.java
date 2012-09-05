package org.weso.utils;

/**
 * Modes accepted in McCSplat Algorithm
 * 
 * @author César Luis Alvargonzález
 * 
 *         http://www.weso.es
 * 
 */
public abstract class Mode {
	public static final int PLAIN_VANILLA = 1;
	public static final int SINK_ABSOLUTE = 2;
	public static final int SINK_RELATIVE = 3;
	public static final int PERCENTILE = 4;
	
	public static final int MIN_VALUE = PLAIN_VANILLA;
	public static final int MAX_VALUE = PERCENTILE;
}

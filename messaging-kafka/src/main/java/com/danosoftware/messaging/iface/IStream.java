package com.danosoftware.messaging.iface;

import java.util.List;

/**
 * 
 * @author andrewcarr
 *
 */
public interface IStream<T> 
{
	/**
	 * 
	 */
	public void initialise();
	
	/**
	 * 
	 * @param output
	 */
	public void sendStream( List<T> output );
}

package com.danosoftware.messaging.iface;

import java.util.List;

/**
 * Message processors are required to process a list of messages
 * received by a message consumer.
 * 
 * The allows the message consumer to be decoupled from the
 * processing of the messages.
 * 
 * @author Danny
 *
 * @param <V> - type of message to be processed
 */
public interface IMessagingProcessor<V>
{
	/**
	 * 
	 * @param istream
	 */
	public void initialiseOutputStream( IStream istream );
	
    /**
     * Process a list of supplied messages.
     * 
     * @param messages
     */
    public void process(List<V> messages);
}
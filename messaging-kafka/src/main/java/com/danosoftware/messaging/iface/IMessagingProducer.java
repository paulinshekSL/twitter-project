package com.danosoftware.messaging.iface;

import java.util.List;

/**
 * Specifies the methods that must be supported by implementations of
 * messaging producers.
 * 
 * @author Danny
 *
 * @param <V>
 */
public interface IMessagingProducer<V>
{

    /**
     * Send a list of messages via the messaging producer.
     * 
     * @param messages
     * @throws Exception
     */
    public void send(List<V> messages) throws Exception;

    /**
     * Close message producer.
     */
    public void close();
    
}
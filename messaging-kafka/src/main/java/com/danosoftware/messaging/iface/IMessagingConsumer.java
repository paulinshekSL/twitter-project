package com.danosoftware.messaging.iface;

/**
 * Message consumers will receive and process a list of messages
 * taken off the messaging provider.
 * 
 * The actual processing is normally provided by implementations
 * of IMessagingProcessor.
 * 
 * This allows the message consumer to be decoupled from the
 * processing of the messages.
 * 
 * @author Danny
 */
public interface IMessagingConsumer
{

    /**
     * Consume messages via the messaging consumer.
     */
    public void consume();

    /**
     * Close message consumer.
     */
    public void close();
}
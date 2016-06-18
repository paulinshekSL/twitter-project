package com.danosoftware.messaging.kafka.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.beans.CustomerSale;

/**
 * Converts a Customer Sale into a serialized byte-stream.
 * Used by Kafka producers to send Customer Sale by Kafka.
 * 
 * @author Danny
 *
 */
public class CustomerSaleDeserializer implements Deserializer<CustomerSale>
{
    private static final Logger logger = LoggerFactory.getLogger(CustomerSaleDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        // no action    
    }

    @Override
    public CustomerSale deserialize(String topic, byte[] data)
    {
        /*
         *  TODO - uses standard Java IO for deserialization. Investigate using Kryo for faster deserialization. See:
         *  https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/
         */
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data); ObjectInput in = new ObjectInputStream(bis);)
        {
            Object o = in.readObject();
            if (o instanceof CustomerSale)
            {
                return (CustomerSale) o;
            }
            else
            {
                logger.error("Unknown instance type while deserializing CustomerSale.");
                return null;
            }
        }
        catch (IOException | ClassNotFoundException e)
        {
            logger.error("Error while deserializing CustomerSale.", e);
            return null;
        }
    }

    @Override
    public void close()
    {
        // no action      
    }
}

package com.danosoftware.messaging.kafka.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.beans.CustomerSale;

/**
 * Converts a Customer Sales into a serialized byte-stream.
 * Used by Kafka producers to send Customer Sales by Kafka.
 * 
 * @author Danny
 *
 */
public class CustomerSaleSerializer implements Serializer<CustomerSale>
{
    private static final Logger logger = LoggerFactory.getLogger(CustomerSaleSerializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        // no action   
    }

    @Override
    public byte[] serialize(String topic, CustomerSale data)
    {
        if (data == null)
        {
            return null;
        }

        /*
         *  TODO - uses standard Java IO for serialization. Investigate using Kryo for faster serialization. See:
         *  https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/
         */
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos);)
        {
            out.writeObject(data);
            return bos.toByteArray();
        }
        catch (IOException e)
        {
            logger.error("Error while serializing CustomerSale.", e);
            return null;
        }
    }

    @Override
    public void close()
    {
        // no action   
    }
}

package com.danosoftware.spark.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.danosoftware.messaging.dto.CustomerSale;

/**
 * Converts a serialized byte-stream into a CustomerSale.
 * Used by Spark stream consumers to decode a CustomerSale from Kafka.
 * 
 * @author Danny
 *
 */
public class CustomerSaleDecoder implements Decoder<CustomerSale>
{
    private static final Logger logger = LoggerFactory.getLogger(CustomerSaleDecoder.class);

    /**
     * Decoder constructor. Decoder implementations MUST be constructed with a VerifiableProperties
     * argument - even if it is not used.
     * 
     * @param props
     */
    public CustomerSaleDecoder(VerifiableProperties props)
    {
        // no action
    }

    @Override
    public CustomerSale fromBytes(byte[] data)
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
}

package com.danosoftware.messaging.kafka.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class StringTestUtilities
{
    // number of messages
    private static final int NUMBER_MESSAGES = 100;

    // possible strings to send
    private static final String MESSAGES[] =
    { "the", "quick", "brown", "fox", "jumped,", "over", "lazy", "dog" };

    /**
     * Return a list of randomly generated strings.
     * 
     * @return
     */
    public static List<String> generateRandomMessages()
    {
        List<String> messages = new ArrayList<>();

        for (int i = 0; i < NUMBER_MESSAGES; i++)
        {
            messages.add(UUID.randomUUID().toString());
        }

        return messages;
    }

    /**
     * Generate list of messages from list
     */
    public static List<String> generateMessages()
    {
        List<String> messages = new ArrayList<>();

        int possibleMessages = MESSAGES.length;
        Random rand = new Random();

        for (int i = 0; i < NUMBER_MESSAGES; i++)
        {
            int messageIndex = rand.nextInt(possibleMessages - 1);
            messages.add(MESSAGES[messageIndex]);
        }

        return messages;
    }
}

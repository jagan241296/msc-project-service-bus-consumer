package org.example;

import java.io.IOException;
import java.util.logging.LogManager;

public class Main {
    public static void main(String[] args) throws IOException {

        loadLogger();

        System.out.println("Consuming from Service Bus...Press Ctrl+C to exit");
        new Consumer().startConsumer();

    }

    private static void loadLogger() throws IOException {
        // Load logging configuration from the logging.properties file
        LogManager.getLogManager().readConfiguration(
                Main.class.getResourceAsStream("/logging.properties")
        );
    }
}
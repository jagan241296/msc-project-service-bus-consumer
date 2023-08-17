package org.example;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.microsoft.applicationinsights.TelemetryClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

import java.util.logging.Logger;

public class Consumer {

    private static final Logger logger = Logger.getLogger(Consumer.class.getName());

    private static final String CUSTOM_LATENCY_METRIC_KEY = "custom.service.bus.latency";

    private final TelemetryClient telemetryClient;
    private final StatsDClient statsDClient;

    public Consumer() {
        this.telemetryClient = new TelemetryClient();
        this.statsDClient = createStatsDClient();
    }

    private ServiceBusProcessorClient constructServiceBusProcessorClient() {
        var connectionString
                = "Endpoint=sb://msc-stream-data.servicebus.windows.net/;SharedAccessKeyName=ClientSender;SharedAccessKey=GD+ToPA+GRfQh9pROLFfC8rlugaO8s5nf+ASbP2pKlo=";
        var topicName = "servicebus_streamdata_topic";
        var subscriptionName = "streamData_subscriber";

        return new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .processor()
                .topicName(topicName)
                .subscriptionName(subscriptionName)
                .receiveMode(ServiceBusReceiveMode.RECEIVE_AND_DELETE)
                .prefetchCount(600)
                .processMessage(this::processMessage)
                .processError(this::processError)
                .buildProcessorClient();
    }

    public void startConsumer() {
        try (var processorClient = constructServiceBusProcessorClient()) {
            processorClient.start();

            // Keep the application running to continue processing messages
            while (true) {
                Thread.sleep(30000);

                System.out.println("Flushing Telemetry Buffer");
                telemetryClient.flush();
            }
        } catch (RuntimeException | InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    // This method is called for each message received from the subscription.
    private void processMessage(ServiceBusReceivedMessageContext context) {
        var endTime = System.currentTimeMillis();
        var message = context.getMessage();
        var startTime = (long) message.getApplicationProperties().get("serviceBus.start-time");
        var latency = endTime - startTime;
        System.out.printf("Received message Seq num: %s, Size: %s, Latency(ms): %s \n",
                message.getSequenceNumber(),
                message.getBody().getLength(),
                latency);


        // send Track Events to App Insights and Datadog
        statsDClient.recordDistributionValue(CUSTOM_LATENCY_METRIC_KEY, latency);
        telemetryClient.trackMetric(CUSTOM_LATENCY_METRIC_KEY, latency);
    }

    // This method is called when there's an error while processing a message.
    private void processError(ServiceBusErrorContext context) {
        System.err.println("Error occurred while processing message: " + context.getException());
    }

    private StatsDClient createStatsDClient() {
        logger.info("Connecting to Datadog stats client");
        return new NonBlockingStatsDClientBuilder()
                .prefix("statsD")
                .hostname("localhost")
                .port(8125)
                .processorWorkers(2)
                .build();
    }
}

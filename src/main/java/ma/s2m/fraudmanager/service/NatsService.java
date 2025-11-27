package ma.s2m.fraudmanager.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import ma.s2m.auth.query.FraudQueryResponse;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.serializer.SerializationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class NatsService {
    private static final Logger logger = LoggerFactory.getLogger(NatsService.class);
    private final Connection nc;
    private final ExecutorService executor;
    private final FraudProcessor fraudProcessor;
    private final QueryProcessor queryProcessor;
    @SuppressWarnings("unused")
    private final RocksDBService rocksDBService;
    private final String topic;
    private final String queryTopic;

    // garder une référence pour pouvoir se désabonner/stopper
    private Dispatcher dispatcher;
    private Dispatcher queryDispatcher;

    public NatsService(Connection nc, ExecutorService executor,
            FraudProcessor fraudProcessor, QueryProcessor queryProcessor, RocksDBService rocksDBService,
            Properties props) {
        this.nc = nc;
        this.executor = executor;
        this.fraudProcessor = fraudProcessor;
        this.queryProcessor = queryProcessor;
        this.rocksDBService = rocksDBService;
        this.topic = AppConfig.natsTopic;
        this.queryTopic = AppConfig.fraudQueryTopic;
    }

    /**
     * Starts the NATS consumer. Each incoming message is submitted to the
     * virtual‑thread executor
     * directly, eliminating the intermediate queue and worker‑loop indirection.
     */
    public void startConsumer() {
        // Dispatcher for main fraud processing
        this.dispatcher = nc.createDispatcher(msg -> {
            executor.submit(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    msg.getHeaders().put("x-recv-ts-ms", String.valueOf(startTime));
                    fraudProcessor.process(msg);
                    long endTime = System.currentTimeMillis();
                    String correlationId = msg.getHeaders() == null ? null
                            : msg.getHeaders().getFirst("x-correlation-id");
                    logger.debug(
                            "Time {} [{}] [{}] [Thread {}] Time taken to process the whole message from NATS receive to end of processing",
                            (endTime - startTime), correlationId, msg.getSubject(), Thread.currentThread().getName());
                } catch (Exception e) {
                    logger.error("Worker error while processing message", e);
                }
            });
        });
        dispatcher.subscribe(topic, "fraudmanager-group");

        // Dispatcher for fraud query topic – starts a placeholder thread on message
        // arrival
        this.queryDispatcher = nc.createDispatcher(msg -> {
            executor.submit(() -> {
                try {
                    queryProcessor.process(msg);
                } catch (Exception e) {
                    logger.error("Worker error while processing message", e);
                    FraudQueryResponse response = new FraudQueryResponse();
                    response.setError(true);
                    response.setErrorMessage(e.getMessage());
                    try {
                        byte[] data = SerializationManager.serialize(response);
                        nc.publish(msg.getSubject(), data);
                    } catch (IOException e1) {
                        logger.error("Error while serializing / publishing message to Nats", e1);
                        e1.printStackTrace();
                    }
                }
            });
        });
        queryDispatcher.subscribe(queryTopic, "fraudmanager-query-group");

        logger.info("Started virtual‑thread executor for processing NATS messages and query dispatcher");

    }

    /**
     * Stops the NATS service, unsubscribes from topics and releases resources.
     */
    public void stop() {
        try {
            if (dispatcher != null) {
                dispatcher.unsubscribe(topic);
                dispatcher = null;
            }
            if (queryDispatcher != null) {
                queryDispatcher.unsubscribe(AppConfig.fraudQueryTopic);
                queryDispatcher = null;
            }
        } catch (Exception e) {
            logger.warn("Error while unsubscribing dispatcher", e);
        }

        // Release processor resources
        fraudProcessor.shutdown();

        // Stop the executor
        executor.shutdownNow();

        try {
            nc.flush(java.time.Duration.ofSeconds(2));
        } catch (Exception ignore) {
        }
        logger.info("NatsService stopped");
    }
}

package ma.s2m.fraudmanager.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import ma.s2m.auth.query.FraudQueryResponse;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.serializer.SerializationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class NatsService {
    private static final Logger logger = LoggerFactory.getLogger(NatsService.class);
    private final Connection nc;
    private final ExecutorService executor;
    private final FraudProcessor fraudProcessor;
    private final QueryProcessor queryProcessor;
    @SuppressWarnings("unused")
    private final IStoreService storageService;
    private final String topic;
    private final String queryTopic;

    // garder une référence pour pouvoir se désabonner/stopper
    private Dispatcher dispatcher;
    private Dispatcher queryDispatcher;

    public NatsService(Connection nc, ExecutorService executor, FraudProcessor fraudProcessor, QueryProcessor queryProcessor, IStoreService storageService) {
        this.nc = nc;
        this.executor = executor;
        this.fraudProcessor = fraudProcessor;
        this.queryProcessor = queryProcessor;
        this.storageService = storageService;
        this.topic = AppConfig.natsTopic;
        this.queryTopic = AppConfig.natsQueryTopic;
    }

    public IStoreService getStorageService() {
        return storageService;
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
                    if (msg.getHeaders() != null) {
                        msg.getHeaders().put("x-recv-ts-ms", String.valueOf(startTime));
                    }
                    fraudProcessor.process(msg);
                    
                    // Flush accumulated writes to reduce serialization overhead
                    storageService.flushBatch();
                    
                    long endTime = System.currentTimeMillis();
                    String correlationId = msg.getHeaders() == null ? null
                            : msg.getHeaders().getFirst("x-correlation-id");
                    logger.debug("Time {} [{}] [{}] [Thread {}] Time taken to process the whole message from NATS receive to end of processing", (endTime - startTime), correlationId, msg.getSubject(), Thread.currentThread().getName());
                } catch (Exception e) {
                    logger.error("Worker error while processing message", e);
                }
            });
        });
        
        if (!AppConfig.fraudManagerMultiNode) {
            // Single-node mode: subscribe to single topic
            dispatcher.subscribe(topic, "fraudmanager-group");
            logger.info("Subscribed to single topic: {}", topic);
        } else {
            // Multi-node mode: subscribe to one topic per shard
            for (Integer shardId : AppConfig.storageShards) {
                String shardTopic = "fraud.check." + shardId;
                dispatcher.subscribe(shardTopic, "fraudmanager-group");
                logger.info("{} subscribed to shard topic: {}", AppConfig.nodeName, shardTopic);
            }
        }

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
        
        if (!AppConfig.fraudManagerMultiNode) {
            queryDispatcher.subscribe(queryTopic, "fraudmanager-query-group");
            logger.info("Subscribed to single query topic: {}", queryTopic);
        } else {
            for (Integer shardId : AppConfig.storageShards) {
                String shardQueryTopic = queryTopic + "." + shardId;
                queryDispatcher.subscribe(shardQueryTopic, "fraudmanager-query-group");
                logger.info("{} subscribed to shard query topic: {}", AppConfig.nodeName, shardQueryTopic);
            }
        }

        logger.info(AppConfig.nodeName + ": Started virtual‑thread executor for processing NATS messages and query dispatcher");

    }

    /**
     * Stops the NATS service, unsubscribes from topics and releases resources.
     */
    public void stop() {
        try {
            if (dispatcher != null) {
                // The dispatcher will automatically unsubscribe from all topics when we call unsubscribe
                // No need to track individual topics
                dispatcher = null;
            }
            if (queryDispatcher != null) {
                queryDispatcher.unsubscribe(AppConfig.natsQueryTopic);
                queryDispatcher = null;
            }
        } catch (Exception e) {
            logger.warn("Error while unsubscribing dispatcher", e);
        }

        // Release processor resources
        fraudProcessor.shutdown();

        // Stop the executor if not done before
        executor.shutdownNow();

        try {
            nc.flush(java.time.Duration.ofSeconds(2));
        } catch (Exception ignore) {
        }
        logger.info("NatsService stopped");
    }
}

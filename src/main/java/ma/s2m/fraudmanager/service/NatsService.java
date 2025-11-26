package ma.s2m.fraudmanager.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import ma.s2m.fraudmanager.config.AppConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class NatsService {
    private static final Logger logger = LoggerFactory.getLogger(NatsService.class);
    private final Connection nc;
    private final ExecutorService executor;
    private final FraudProcessor processor;
    @SuppressWarnings("unused")
    private final RocksDBService rocksDBService;
    private final String topic;

    // garder une référence pour pouvoir se désabonner/stopper
    private Dispatcher dispatcher;

    public NatsService(Connection nc, ExecutorService executor,
            FraudProcessor processor, RocksDBService rocksDBService, Properties props) {
        this.nc = nc;
        this.executor = executor;
        this.processor = processor;
        this.rocksDBService = rocksDBService;
        this.topic = AppConfig.natsTopic;
    }

    /**
     * Starts the NATS consumer. Each incoming message is submitted to the
     * virtual‑thread executor
     * directly, eliminating the intermediate queue and worker‑loop indirection.
     */
    public void startConsumer() {
        this.dispatcher = nc.createDispatcher(msg -> {
            executor.submit(() -> {
                try {
                    long startTime = System.currentTimeMillis();
                    msg.getHeaders().put("x-recv-ts-ms", String.valueOf(startTime));
                    processor.process(msg);
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
        logger.info("Started virtual‑thread executor for processing NATS messages");
    }

    // arrêt propre à appeler lors du shutdown de l’appli
    public void stop() {
        try {
            if (dispatcher != null) {
                dispatcher.unsubscribe(topic);
                dispatcher = null;
            }
        } catch (Exception e) {
            logger.warn("Error while unsubscribing dispatcher", e);
        }

        // interrompre les workers
        executor.shutdownNow(); // provoque InterruptedException

        // libérer les ressources du processor (sessions Drools, pool interne, etc.)
        processor.shutdown();

        try {
            // drainer/flush NATS si nécessaire
            nc.flush(java.time.Duration.ofSeconds(2));
        } catch (Exception ignore) {
        }
        logger.info("NatsService stopped");
    }
}

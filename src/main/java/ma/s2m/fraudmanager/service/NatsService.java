package ma.s2m.fraudmanager.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import ma.s2m.fraudmanager.config.AppConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class NatsService {
    private static final Logger logger = LoggerFactory.getLogger(NatsService.class);
    private final Connection nc;
    private final BlockingQueue<Message> queue;
    private final ExecutorService executor;
    private final FraudProcessor processor;
    private final String topic;
    private final int threadPoolSize; // Ajouté pour stocker la taille du pool

    public NatsService(Connection nc, BlockingQueue<Message> queue, ExecutorService executor, FraudProcessor processor, Properties props) {
        this.nc = nc;
        this.queue = queue;
        this.executor = executor;
        this.processor = processor;
        this.topic = AppConfig.natsTopic;
        this.threadPoolSize = AppConfig.appThreadPoolSize;
    }

    public void startConsumer() {
        Dispatcher dispatcher = nc.createDispatcher((msg) -> {
            try {
                queue.put(msg);  // Ajoute à la queue pour backpressure
            } catch (InterruptedException e) {
                logger.error("Error adding to queue", e);
                Thread.currentThread().interrupt(); // Rétablir l'état d'interruption
            }
        });
        dispatcher.subscribe(topic, "fraudmanager-group");

        // Démarre les workers en fonction de threadPoolSize
        for (int i = 0; i < threadPoolSize; i++) {
            executor.submit(() -> {
                while (true) {
                    try {
                        Message msg = queue.take();
                        processor.process(msg);
                    } catch (InterruptedException e) {
                        logger.error("Worker interrupted", e);
                        Thread.currentThread().interrupt(); // Rétablir l'état d'interruption
                    }
                }
            });
        }
        logger.info("Started {} worker threads for processing transactions", threadPoolSize);
    }
}
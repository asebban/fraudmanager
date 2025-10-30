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
    private final int threadPoolSize;

    // garder une référence pour pouvoir se désabonner/stopper
    private Dispatcher dispatcher;

    public NatsService(Connection nc, BlockingQueue<Message> queue, ExecutorService executor,
                       FraudProcessor processor, Properties props) {
        this.nc = nc;
        this.queue = queue;
        this.executor = executor;
        this.processor = processor;
        this.topic = AppConfig.natsTopic;
        this.threadPoolSize = AppConfig.appThreadPoolSize;
    }

    public void startConsumer() {
        this.dispatcher = nc.createDispatcher(msg -> {
            try {
                queue.put(msg); // backpressure
                logger.debug("Worker thread {} got a message, Queue size: {}", Thread.currentThread().getName(), queue.size());
            } catch (InterruptedException e) {
                logger.error("Error adding to queue", e);
                Thread.currentThread().interrupt();
            }
        });
        dispatcher.subscribe(topic, "fraudmanager-group");

        for (int i = 0; i < threadPoolSize; i++) {
            executor.submit(() -> {
                // boucle interrompable
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Message msg = queue.take();
                        Long startTime = System.currentTimeMillis();
                        processor.process(msg);
                        Long endTime = System.currentTimeMillis();
                        String correlationId = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-correlation-id");
                        logger.debug("Time {} [{}] [{}] [Thread {}] Time taken to process the whole message from NATS receive to end of processing",
                                (endTime - startTime), correlationId, msg.getSubject(), Thread.currentThread().getName());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break; // sortir proprement
                    } catch (Exception e) {
                        logger.error("Worker error while processing message", e);
                    }
                }
                logger.info("Worker thread {} stopped", Thread.currentThread().getName());
            });
        }
        logger.info("Started {} worker threads for processing transactions", threadPoolSize);
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
        executor.shutdownNow(); // provoque InterruptedException dans queue.take()

        // libérer les ressources du processor (sessions Drools, pool interne, etc.)
        processor.shutdown();

        try {
            // drainer/flush NATS si utile
            nc.flush(java.time.Duration.ofSeconds(2));
        } catch (Exception ignore) {}
        logger.info("NatsService stopped");
    }
}

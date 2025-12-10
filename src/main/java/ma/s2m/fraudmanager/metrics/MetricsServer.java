package ma.s2m.fraudmanager.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public final class MetricsServer {
    private static final Logger logger = LoggerFactory.getLogger(MetricsServer.class);

    private static HttpServer server;
    private static PrometheusMeterRegistry registry;

    private MetricsServer() {}

    public static synchronized MeterRegistry start(int port, String path) {
        if (server != null) {
            return registry;
        }
        try {
            registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
            server.createContext(path, new MetricsHandler(registry));
            server.setExecutor(java.util.concurrent.Executors.newSingleThreadExecutor());
            server.start();
            logger.info("Metrics server started on 0.0.0.0:{}{}", port, path);
            return registry;
        } catch (IOException e) {
            logger.warn("Failed to start metrics server on {}{}", port, path, e);
            return null;
        }
    }

    public static synchronized void stop() {
        if (server != null) {
            try {
                server.stop(0);
                logger.info("Metrics server stopped");
            } catch (Exception ignore) {}
            server = null;
        }
    }

    private static class MetricsHandler implements HttpHandler {
        private final PrometheusMeterRegistry reg;

        MetricsHandler(PrometheusMeterRegistry reg) {
            this.reg = reg;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String body = reg.scrape();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}

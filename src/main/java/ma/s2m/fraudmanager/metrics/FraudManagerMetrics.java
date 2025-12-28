package ma.s2m.fraudmanager.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FraudManagerMetrics {

    private static FraudManagerMetrics instance;
    private final MeterRegistry registry;

    // Counters
    private final Counter natsMessagesReceived;
    private final Counter natsMessagesSent;
    private final Counter transactionsFailed;
    private final Counter alertsGenerated;
    private final Counter serializationOperations;
    private final Counter deserializationOperations;
    private final Map<String, Counter> alertsByRule;
    private final Map<String, Counter> transactionsBySubject;

    // Timers
    private final Timer transactionProcessingTime;
    private final Timer droolsExecutionTime;
    private final Timer sessionAcquireTime;
    private final Timer serializationTime;

    // Gauges
    private final AtomicInteger activeTransactions;
    private final AtomicInteger activeRules;

    // Summaries
    private final DistributionSummary messageSize;
    private final DistributionSummary transactionAmount;
    private final DistributionSummary alertScore;

    private FraudManagerMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.alertsByRule = new ConcurrentHashMap<>();
        this.transactionsBySubject = new ConcurrentHashMap<>();

        // Initialize Counters
        this.natsMessagesReceived = Counter.builder("fraudmanager.nats.messages.received.total")
            .description("Total number of NATS messages received")
            .register(registry);
        
        this.natsMessagesSent = Counter.builder("fraudmanager.nats.messages.sent.total")
            .description("Total number of NATS messages sent")
            .register(registry);

        this.transactionsFailed = Counter.builder("fraudmanager.transactions.failed.total")
            .description("Total number of failed transactions")
            .register(registry);

        this.alertsGenerated = Counter.builder("fraudmanager.alerts.generated.total")
            .description("Total number of alerts generated")
            .register(registry);

        this.serializationOperations = Counter.builder("fraudmanager.serialization.operations.total")
            .description("Total number of serialization operations")
            .register(registry);

        this.deserializationOperations = Counter.builder("fraudmanager.deserialization.operations.total")
            .description("Total number of deserialization operations")
            .register(registry);

        // Initialize Timers
        this.transactionProcessingTime = Timer.builder("fraudmanager.transaction.processing.time.seconds")
            .description("Time taken to process a transaction end-to-end")
            .register(registry);

        this.droolsExecutionTime = Timer.builder("fraudmanager.drools.execution.time.seconds")
            .description("Time taken for Drools rule execution")
            .register(registry);

        this.sessionAcquireTime = Timer.builder("fraudmanager.session.acquire.time.seconds")
            .description("Time taken to acquire a Drools session")
            .register(registry);

        this.serializationTime = Timer.builder("fraudmanager.serialization.time.seconds")
            .description("Time taken for serialization/deserialization")
            .register(registry);

        // Initialize Gauges
        this.activeTransactions = new AtomicInteger(0);
        Gauge.builder("fraudmanager.transactions.active", activeTransactions, AtomicInteger::get)
            .description("Number of currently active transactions")
            .register(registry);

        this.activeRules = new AtomicInteger(0);
        Gauge.builder("fraudmanager.rules.active", activeRules, AtomicInteger::get)
            .description("Number of active rules loaded")
            .register(registry);

        // Initialize Summaries
        this.messageSize = DistributionSummary.builder("fraudmanager.message.size.bytes")
            .description("Size of NATS messages")
            .baseUnit("bytes")
            .register(registry);

        this.transactionAmount = DistributionSummary.builder("fraudmanager.transaction.amount")
            .description("Transaction amounts processed")
            .register(registry);

        this.alertScore = DistributionSummary.builder("fraudmanager.alert.score")
            .description("Score of generated alerts")
            .register(registry);
    }

    public static synchronized void init(MeterRegistry registry) {
        if (instance == null) {
            instance = new FraudManagerMetrics(registry);
        }
    }

    public static FraudManagerMetrics getInstance() {
        return instance;
    }

    // NATS Metrics
    public void incrementNatsMessagesReceived() {
        natsMessagesReceived.increment();
    }

    public void incrementNatsMessagesSent() {
        natsMessagesSent.increment();
    }

    public void incrementTransactionsFailed() {
        transactionsFailed.increment();
    }

    public void incrementAlertsGenerated(double amount) {
        alertsGenerated.increment(amount);
    }

    public void incrementSerializationOperations() {
        serializationOperations.increment();
    }

    public void incrementDeserializationOperations() {
        deserializationOperations.increment();
    }

    public void incrementAlertsByRule(String ruleName) {
        alertsByRule.computeIfAbsent(ruleName, key -> 
            Counter.builder("fraudmanager.alerts.by.rule.total")
                .tag("rule", key)
                .description("Alerts generated by specific rule")
                .register(registry)
        ).increment();
    }

    public void incrementTransactionsBySubject(String subject) {
        transactionsBySubject.computeIfAbsent(subject, key -> 
            Counter.builder("fraudmanager.transactions.by.subject.total")
                .tag("subject", key)
                .description("Transactions processed by subject type")
                .register(registry)
        ).increment();
    }

    // Timer methods
    public Timer.Sample startTransactionTimer() {
        return Timer.start(registry);
    }

    public void recordTransactionTime(Timer.Sample sample) {
        sample.stop(transactionProcessingTime);
    }

    public Timer.Sample startDroolsTimer() {
        return Timer.start(registry);
    }

    public void recordDroolsTime(Timer.Sample sample) {
        sample.stop(droolsExecutionTime);
    }

    public Timer.Sample startSessionAcquireTimer() {
        return Timer.start(registry);
    }

    public void recordSessionAcquireTime(Timer.Sample sample) {
        sample.stop(sessionAcquireTime);
    }

    public Timer.Sample startSerializationTimer() {
        return Timer.start(registry);
    }

    public void recordSerializationTime(Timer.Sample sample) {
        sample.stop(serializationTime);
    }

    // Gauge methods
    public void incrementActiveTransactions() {
        activeTransactions.incrementAndGet();
    }

    public void decrementActiveTransactions() {
        activeTransactions.decrementAndGet();
    }

    public void setActiveRules(int count) {
        activeRules.set(count);
    }

    // Summary methods
    public void recordMessageSize(double size) {
        messageSize.record(size);
    }

    public void recordTransactionAmount(double amount) {
        transactionAmount.record(amount);
    }

    public void recordAlertScore(double score) {
        alertScore.record(score);
    }
}

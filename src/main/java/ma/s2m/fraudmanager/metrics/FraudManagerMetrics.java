package ma.s2m.fraudmanager.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Centralized metrics for the FraudManager application.
 * Provides counters, timers, gauges, and distribution summaries for monitoring.
 */
public final class FraudManagerMetrics {

    private static volatile FraudManagerMetrics instance;
    private final MeterRegistry registry;

    // === Counters ===
    private final Counter transactionsProcessed;
    private final Counter transactionsFailed;
    private final Counter alertsGenerated;
    private final Counter rulesTriggered;
    private final Counter natsMessagesReceived;
    private final Counter natsMessagesSent;
    private final Counter storageReads;
    private final Counter storageWrites;
    private final Counter serializationOperations;
    private final Counter deserializationOperations;

    // === Timers ===
    private final Timer transactionProcessingTime;
    private final Timer droolsExecutionTime;
    private final Timer storageReadTime;
    private final Timer storageWriteTime;
    private final Timer serializationTime;
    private final Timer natsResponseTime;
    private final Timer sessionAcquireTime;

    // === Distribution Summaries ===
    private final DistributionSummary transactionAmountSummary;
    private final DistributionSummary alertScoreSummary;
    private final DistributionSummary windowSizeSummary;
    private final DistributionSummary messageSizeSummary;

    // === Atomic values for Gauges ===
    private final AtomicLong activeTransactions = new AtomicLong(0);
    private final AtomicLong queuedMessages = new AtomicLong(0);
    private final AtomicLong activeRulesCount = new AtomicLong(0);
    private final AtomicLong activeSessions = new AtomicLong(0);

    // === Subject-specific counters ===
    private final ConcurrentHashMap<String, Counter> subjectCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> alertsByRule = new ConcurrentHashMap<>();

    private FraudManagerMetrics(MeterRegistry registry) {
        this.registry = registry;

        // === Initialize Counters ===
        this.transactionsProcessed = Counter.builder("fraudmanager.transactions.processed")
                .description("Total number of transactions processed")
                .register(registry);

        this.transactionsFailed = Counter.builder("fraudmanager.transactions.failed")
                .description("Total number of transactions that failed processing")
                .register(registry);

        this.alertsGenerated = Counter.builder("fraudmanager.alerts.generated")
                .description("Total number of alerts generated")
                .register(registry);

        this.rulesTriggered = Counter.builder("fraudmanager.rules.triggered")
                .description("Total number of rules triggered")
                .register(registry);

        this.natsMessagesReceived = Counter.builder("fraudmanager.nats.messages.received")
                .description("Total NATS messages received")
                .register(registry);

        this.natsMessagesSent = Counter.builder("fraudmanager.nats.messages.sent")
                .description("Total NATS messages sent")
                .register(registry);

        this.storageReads = Counter.builder("fraudmanager.storage.reads")
                .description("Total storage read operations")
                .register(registry);

        this.storageWrites = Counter.builder("fraudmanager.storage.writes")
                .description("Total storage write operations")
                .register(registry);

        this.serializationOperations = Counter.builder("fraudmanager.serialization.operations")
                .description("Total Kryo serialization operations")
                .register(registry);

        this.deserializationOperations = Counter.builder("fraudmanager.deserialization.operations")
                .description("Total Kryo deserialization operations")
                .register(registry);

        // === Initialize Timers ===
        this.transactionProcessingTime = Timer.builder("fraudmanager.transaction.processing.time")
                .description("Time to process a single transaction")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.droolsExecutionTime = Timer.builder("fraudmanager.drools.execution.time")
                .description("Time spent executing Drools rules")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.storageReadTime = Timer.builder("fraudmanager.storage.read.time")
                .description("Time for storage read operations")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.storageWriteTime = Timer.builder("fraudmanager.storage.write.time")
                .description("Time for storage write operations")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.serializationTime = Timer.builder("fraudmanager.serialization.time")
                .description("Time spent on Kryo serialization/deserialization")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.natsResponseTime = Timer.builder("fraudmanager.nats.response.time")
                .description("NATS request-response round trip time")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.sessionAcquireTime = Timer.builder("fraudmanager.session.acquire.time")
                .description("Time to acquire a Drools session from the pool")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        // === Initialize Distribution Summaries ===
        this.transactionAmountSummary = DistributionSummary.builder("fraudmanager.transaction.amount")
                .description("Distribution of transaction amounts")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.alertScoreSummary = DistributionSummary.builder("fraudmanager.alert.score")
                .description("Distribution of alert scores")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.windowSizeSummary = DistributionSummary.builder("fraudmanager.window.entries")
                .description("Number of entries in sliding windows")
                .register(registry);

        this.messageSizeSummary = DistributionSummary.builder("fraudmanager.message.size.bytes")
                .description("Size of NATS messages in bytes")
                .register(registry);

        // === Initialize Gauges ===
        Gauge.builder("fraudmanager.transactions.active", activeTransactions, AtomicLong::get)
                .description("Number of transactions currently being processed")
                .register(registry);

        Gauge.builder("fraudmanager.queue.messages", queuedMessages, AtomicLong::get)
                .description("Number of messages waiting in the queue")
                .register(registry);

        Gauge.builder("fraudmanager.rules.active", activeRulesCount, AtomicLong::get)
                .description("Number of active fraud detection rules")
                .register(registry);

        Gauge.builder("fraudmanager.sessions.active", activeSessions, AtomicLong::get)
                .description("Number of active Drools sessions")
                .register(registry);
    }

    /**
     * Initialize the singleton instance with a MeterRegistry.
     */
    public static void init(MeterRegistry registry) {
        if (instance == null) {
            synchronized (FraudManagerMetrics.class) {
                if (instance == null) {
                    instance = new FraudManagerMetrics(registry);
                }
            }
        }
    }

    /**
     * Get the singleton instance.
     */
    public static FraudManagerMetrics getInstance() {
        return instance;
    }

    // === Counter Methods ===

    public void incrementTransactionsProcessed() {
        transactionsProcessed.increment();
    }

    public void incrementTransactionsFailed() {
        transactionsFailed.increment();
    }

    public void incrementAlertsGenerated() {
        alertsGenerated.increment();
    }

    public void incrementAlertsGenerated(int count) {
        alertsGenerated.increment(count);
    }

    public void incrementRulesTriggered() {
        rulesTriggered.increment();
    }

    public void incrementNatsMessagesReceived() {
        natsMessagesReceived.increment();
    }

    public void incrementNatsMessagesSent() {
        natsMessagesSent.increment();
    }

    public void incrementStorageReads() {
        storageReads.increment();
    }

    public void incrementStorageWrites() {
        storageWrites.increment();
    }

    public void incrementSerializationOperations() {
        serializationOperations.increment();
    }

    public void incrementDeserializationOperations() {
        deserializationOperations.increment();
    }

    // === Subject-specific counter ===
    public void incrementTransactionsBySubject(String subject) {
        subjectCounters.computeIfAbsent(subject, s ->
                Counter.builder("fraudmanager.transactions.by_subject")
                        .tag("subject", s)
                        .description("Transactions processed by subject type")
                        .register(registry)
        ).increment();
    }

    // === Rule-specific alert counter ===
    public void incrementAlertsByRule(String ruleName) {
        alertsByRule.computeIfAbsent(ruleName, r ->
                Counter.builder("fraudmanager.alerts.by_rule")
                        .tag("rule", r)
                        .description("Alerts generated by specific rule")
                        .register(registry)
        ).increment();
    }

    // === Timer Methods ===

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

    public Timer.Sample startStorageReadTimer() {
        return Timer.start(registry);
    }

    public void recordStorageReadTime(Timer.Sample sample) {
        sample.stop(storageReadTime);
    }

    public Timer.Sample startStorageWriteTimer() {
        return Timer.start(registry);
    }

    public void recordStorageWriteTime(Timer.Sample sample) {
        sample.stop(storageWriteTime);
    }

    public Timer.Sample startSerializationTimer() {
        return Timer.start(registry);
    }

    public void recordSerializationTime(Timer.Sample sample) {
        sample.stop(serializationTime);
    }

    public Timer.Sample startNatsTimer() {
        return Timer.start(registry);
    }

    public void recordNatsResponseTime(Timer.Sample sample) {
        sample.stop(natsResponseTime);
    }

    public Timer.Sample startSessionAcquireTimer() {
        return Timer.start(registry);
    }

    public void recordSessionAcquireTime(Timer.Sample sample) {
        sample.stop(sessionAcquireTime);
    }

    // === Convenience method for timing a Runnable ===
    public void timeTransaction(Runnable action) {
        transactionProcessingTime.record(action);
    }

    public <T> T timeTransaction(Supplier<T> action) {
        return transactionProcessingTime.record(action);
    }

    public void timeDroolsExecution(Runnable action) {
        droolsExecutionTime.record(action);
    }

    // === Distribution Summary Methods ===

    public void recordTransactionAmount(double amount) {
        transactionAmountSummary.record(amount);
    }

    public void recordAlertScore(double score) {
        alertScoreSummary.record(score);
    }

    public void recordWindowEntries(long entries) {
        windowSizeSummary.record(entries);
    }

    public void recordMessageSize(long bytes) {
        messageSizeSummary.record(bytes);
    }

    // === Gauge Methods ===

    public void incrementActiveTransactions() {
        activeTransactions.incrementAndGet();
    }

    public void decrementActiveTransactions() {
        activeTransactions.decrementAndGet();
    }

    public void setQueuedMessages(long count) {
        queuedMessages.set(count);
    }

    public void setActiveRulesCount(long count) {
        activeRulesCount.set(count);
    }

    public void incrementActiveSessions() {
        activeSessions.incrementAndGet();
    }

    public void decrementActiveSessions() {
        activeSessions.decrementAndGet();
    }

    public void setActiveSessions(long count) {
        activeSessions.set(count);
    }

    // === Custom Gauge Registration ===

    /**
     * Register a custom gauge with a supplier for dynamic values.
     */
    public <T extends Number> void registerGauge(String name, String description, Supplier<T> supplier) {
        Gauge.builder(name, supplier, s -> s.get().doubleValue())
                .description(description)
                .register(registry);
    }

    /**
     * Register a gauge for a specific shard's storage size.
     */
    public void registerShardSizeGauge(int shardId, Supplier<Long> sizeSupplier) {
        Gauge.builder("fraudmanager.storage.shard.size", sizeSupplier, s -> s.get().doubleValue())
                .tag("shard", String.valueOf(shardId))
                .description("Number of entries in storage shard")
                .register(registry);
    }

    /**
     * Register a gauge for session pool size.
     */
    public void registerSessionPoolGauge(String subject, Supplier<Integer> poolSizeSupplier) {
        Gauge.builder("fraudmanager.session.pool.size", poolSizeSupplier, s -> s.get().doubleValue())
                .tag("subject", subject)
                .description("Available sessions in pool for subject")
                .register(registry);
    }
}

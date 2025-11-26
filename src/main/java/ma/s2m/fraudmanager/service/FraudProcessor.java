package ma.s2m.fraudmanager.service;

import ma.medtech.droolbuilder.messaging.IMessageSender;
import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.medtech.droolbuilder.services.TypeConverter;
import ma.medtech.droolbuilder.utils.TimeConversion;
import ma.s2m.auth.Alert;
import ma.s2m.auth.AlertSet;
import ma.s2m.auth.FraudCheckRequest;
import ma.s2m.auth.FraudCheckResponse;
import ma.s2m.auth.ITransaction;
import ma.s2m.auth.impl.VRTransactionSummary;
import ma.s2m.auth.impl.VirtualRecordTransaction;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.config.RulesConfig;
import ma.s2m.fraudmanager.drools.DroolsSession;
import ma.s2m.fraudmanager.drools.DroolsSessionFactory;
import ma.s2m.fraudmanager.drools.SessionMode;
import ma.s2m.fraudmanager.helpers.TransactionDummyHelper;
import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.model.MeasurmentRecord;
import ma.s2m.fraudmanager.model.RecordsDelta;
import ma.s2m.fraudmanager.model.TrxEntry;
import ma.s2m.fraudmanager.model.TrxOrAlertEvent;
import ma.s2m.fraudmanager.util.Subject;
import ma.s2m.serializer.SerializationManager;
import io.nats.client.Connection;
import io.nats.client.Message;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudProcessor {

    private static final String KEY_SEPARATOR = "/";
    private static final String CARD_KEY_PREFIX = Subject.CARD + ":";
    private static final String MERCHANT_KEY_PREFIX = Subject.MERCHANT + ":";
    private static final String CUSTOM_KEY_PREFIX = Subject.CUSTOM + ":";

    private static final Logger logger = LoggerFactory.getLogger(FraudProcessor.class);
    private final RocksDBService rocksDBService;
    private final Connection natsConnection;
    private final KeyProcessor keyProcessor;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor(); // Pool pour traitement
                                                                                          // parallèle
    private IMessageSender messageSender;
    private DroolsSessionFactory sessionFactory;
    private int sessionPoolSize = AppConfig.appThreadSessionPoolSize;
    private Map<String, BlockingQueue<DroolsSession>> sessionPools = new ConcurrentHashMap<>();

    @SuppressWarnings("unused")
    private DroolsSession createNewDroolsSession() {
        HashMap<String, Object> globals = new HashMap<>();
        globals.put("timeConverter", new TimeConversion());
        globals.put("externalSystem", new ExternalSystem());
        globals.put("messageSender", messageSender);
        globals.put("typeConverter", new TypeConverter());

        DroolsSession s = sessionFactory.newSession(SessionMode.STATEFUL, globals);
        return s;
    }

    public FraudProcessor(RocksDBService rocksDBService, Connection natsConnection) throws IOException {
        this.rocksDBService = rocksDBService;
        this.natsConnection = natsConnection;
        this.keyProcessor = new KeyProcessor(rocksDBService);
        initProcessor();
        initializeSessionPools();
    }

    private void initializeSessionPools() {
        Set<String> subjects = Set.of(Subject.CARD, Subject.MERCHANT, Subject.CUSTOM);
        for (String subject : subjects) {
            BlockingQueue<DroolsSession> queue = new ArrayBlockingQueue<>(sessionPoolSize);
            for (int i = 0; i < sessionPoolSize; i++) {
                DroolsSession session = createSessionForSubject(subject);
                queue.offer(session);
            }
            sessionPools.put(subject, queue);
            logger.info("Initialized Drools session pool for subject '{}' with {} sessions", subject,
                    sessionPoolSize);
        }
    }

    private DroolsSession createSessionForSubject(String subject) {
        Map<String, Object> globals = new HashMap<>();
        globals.put("timeConverter", new TimeConversion());
        globals.put("externalSystem", new ExternalSystem());
        globals.put("messageSender", messageSender);
        globals.put("typeConverter", new TypeConverter());
        return sessionFactory.newSession(SessionMode.STATEFUL, globals);
    }

    @SuppressWarnings("deprecation")
    private void initProcessor() throws IOException {

        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new FileNotFoundException("application.properties file not found in resources directory");
            }
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            this.messageSender = (IMessageSender) Class
                    .forName(properties.getProperty("app.processor.messaging.provider",
                            "ma.medtech.droolbuilder.messaging.SysoutMessageSender"))
                    .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException("ServiceFactory: Error while creating message sender", e);
        }

        this.sessionFactory = new DroolsSessionFactory(RulesConfig.extendedVersion);
    }

    private DroolsSession acquireSession(String subject) throws InterruptedException {
        BlockingQueue<DroolsSession> pool = sessionPools.get(subject);
        if (pool == null) {
            throw new IllegalArgumentException("No session pool for subject: " + subject);
        }
        DroolsSession session = pool.take(); // Bloque si vide
        return session;
    }

    private void releaseSession(String subject, DroolsSession session) {
        if (session != null) {
            try {
                session.clean(); // Nettoie les faits
                sessionPools.get(subject).offer(session);
            } catch (Exception e) {
                logger.error("Error returning session to pool for subject: {}", subject, e);
                // Recrée une session en cas d'erreur
                sessionPools.get(subject).offer(createSessionForSubject(subject));
            }
        }
    }

    private void executeSession(Long windowSize, Measurment measurment, String subject, String correlationId)
            throws Exception {
        if (measurment == null) {
            throw new IllegalArgumentException("executeSession: Measurment cannot be null");
        }
        if (measurment.getAlertSet() == null) {
            measurment.setAlertSet(new AlertSet());
        }
        DroolsSession session = null;
        try {
            Long beginAcquireSession = System.currentTimeMillis();
            session = acquireSession(subject);
            Long endAcquireSession = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [{}] win={} key={} ProcessFunction: acquireSession() duration",
                    (endAcquireSession - beginAcquireSession), correlationId, subject,
                    TimeConversion.toHumanReadableDuration(windowSize), measurment.getKey());
            session.execute(measurment, windowSize, subject, correlationId);
        } catch (Exception e) {
            logger.error("Error executing session for subject: {}", subject, e);
        } finally {
            releaseSession(subject, session);
        }
    }

    @SuppressWarnings("unused")
    private void warmUpDrools(String subject, DroolsSession s) {
        VirtualRecordTransaction dummyTrx = TransactionDummyHelper.dummyTransaction();
        Measurment m = createNewMeasument("card-1", subject, 10000L);
        m.setTransaction(new VRTransactionSummary(dummyTrx));
        try {
            // on warm-up la session fournie
            executeSession(10000L, m, subject, "XXXXX");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Measurment createNewMeasument(String key, String subject, Long windowSize) {
        Measurment measurment = new Measurment();
        measurment.setKey(key);
        measurment.setSubject(subject);
        measurment.setWindowSize(windowSize);
        measurment.setTrxEntries(new ArrayList<>());
        return measurment;
    }

    private AlertSet newAlertSet(TrxOrAlertEvent event, String subject) {
        AlertSet alertSet = new AlertSet();
        switch (subject) {
            case Subject.CARD:
                alertSet.setKey(event.getTransaction().getCardId());
                break;
            case Subject.MERCHANT:
                alertSet.setKey(event.getTransaction().getMerchant());
                break;
            case Subject.CUSTOM:
                alertSet.setKey(event.getAlertSet().getKey());
                break;
            default:
                break;
        }
        alertSet.setTopic(event.getTransaction().getTopic());
        alertSet.setAlertStatus(AlertSet.NO_ALERT);
        alertSet.setTransactionNo(event.getTransaction().getTransactionNo());
        return alertSet;
    }

    private TrxOrAlertEvent buildEvent(TrxOrAlertEvent previousEvent, AlertSet alertSet) {

        if (previousEvent.getAlertSet() == null) {
            previousEvent.setTimestamp(System.currentTimeMillis());
            TrxOrAlertEvent event = new TrxOrAlertEvent(previousEvent);
            alertSet.setTopic(previousEvent.getTransaction().getTopic());
            event.setAlertSet(alertSet);
            return event;
        }

        if (alertSet == null || alertSet.getAlerts() == null || alertSet.getAlerts().isEmpty()
                || alertSet.getAlertStatus().equals(AlertSet.NO_ALERT)) {
            // No new alertsets, return the previous one
            previousEvent.setTimestamp(System.currentTimeMillis());
            previousEvent.getAlertSet().setTopic(previousEvent.getTransaction().getTopic());
            return new TrxOrAlertEvent(previousEvent);
        }

        TrxOrAlertEvent event = new TrxOrAlertEvent(previousEvent);

        alertSet.getAlerts().forEach(a -> {
            event.getAlertSet().getAlerts().add(a.copy());
        });

        if (event.getAlertSet().hasAlerts()) {
            event.getAlertSet().setAlertStatus(AlertSet.ALERT);
            event.getAlertSet().calculateScore(RulesConfig.alertRulesCount);
        } else {
            event.getAlertSet().setAlertStatus(AlertSet.NO_ALERT);
            event.getAlertSet().setScore(0.0);
        }
        event.setTimestamp(System.currentTimeMillis());
        event.getAlertSet().setTopic(previousEvent.getTransaction().getTopic());
        VRTransactionSummary trx = new VRTransactionSummary(previousEvent.getTransaction());
        event.setTransaction(trx);
        return event;
    }

    /**
     * Subtracts a double value from the specified attribute in the measurement
     * record.
     *
     * @param entry   The entry containing the attribute key and value.
     * @param record  The measurement record to update.
     * @param attrKey The key of the attribute to subtract the value from.
     */
    private void substractDoubleValue(Entry<String, Object> entry, MeasurmentRecord record, String attrKey) {
        if (entry.getValue() != null && entry.getValue() instanceof Double && (Double) entry.getValue() != 0.0) {
            Double deltaValue = (Double) entry.getValue();
            if (record.getValues().containsKey(attrKey) && record.getValues().get(attrKey) instanceof Double) {
                Double currentValue = (Double) record.getValues().get(attrKey);
                record.getValues().put(attrKey, currentValue - deltaValue);
            }
        }
    }

    /**
     * Subtracts an integer value from the specified attribute in the measurement
     * record.
     *
     * @param entry   The entry containing the attribute key and value.
     * @param record  The measurement record to update.
     * @param attrKey The key of the attribute to subtract the value from.
     */
    private void substractIntegerValue(Entry<String, Object> entry, MeasurmentRecord record, String attrKey) {
        if (entry.getValue() != null && entry.getValue() instanceof Integer && (Integer) entry.getValue() != 0) {
            Integer deltaValue = (Integer) entry.getValue();
            if (record.getValues().containsKey(attrKey) && record.getValues().get(attrKey) instanceof Integer) {
                Integer currentValue = (Integer) record.getValues().get(attrKey);
                record.getValues().put(attrKey, currentValue - deltaValue);
            }
        }
    }

    /**
     * Subtracts a long value from the specified attribute in the measurement
     * record.
     *
     * @param entry   The entry containing the attribute key and value.
     * @param record  The measurement record to update.
     * @param attrKey The key of the attribute to subtract the value from.
     */
    private void substractLongValue(Entry<String, Object> entry, MeasurmentRecord record, String attrKey) {
        if (entry.getValue() != null && entry.getValue() instanceof Long && (Long) entry.getValue() != 0L) {
            Long deltaValue = (Long) entry.getValue();
            if (record.getValues().containsKey(attrKey) && record.getValues().get(attrKey) instanceof Long) {
                Long currentValue = (Long) record.getValues().get(attrKey);
                record.getValues().put(attrKey, currentValue - deltaValue);
            }
        }
    }

    /**
     * Subtracts delta values from the measurement based on the transaction entry.
     *
     * @param m        The measurement to update.
     * @param trxEntry The transaction entry containing delta values.
     */
    private void substractDelta(Measurment m, TrxEntry trxEntry) {

        if (trxEntry == null) {
            return;
        }

        if (m.getRecords() != null && !m.getRecords().isEmpty()) {

            for (String recordKey : m.getRecords().keySet()) {
                MeasurmentRecord record = m.getRecords().get(recordKey);

                if (trxEntry.getRecordDelta() != null && trxEntry.getRecordDelta().get(recordKey) != null) {

                    record.setCount(record.getCount() - trxEntry.getRecordDelta().get(recordKey).getCountDelta());
                    record.setAmount(record.getAmount() - trxEntry.getRecordDelta().get(recordKey).getAmountDelta());

                    for (Entry<String, Object> entry : trxEntry.getRecordDelta().get(recordKey).getValuesDelta()
                            .entrySet()) {
                        String attrKey = entry.getKey();
                        substractDoubleValue(entry, record, attrKey);
                        substractLongValue(entry, record, attrKey);
                        substractIntegerValue(entry, record, attrKey);
                    }

                    if (trxEntry.getRecordDelta().get(recordKey).getArgSetDelta() != null
                            && trxEntry.getRecordDelta().get(recordKey).getArgSetDelta() != null
                            && !trxEntry.getRecordDelta().get(recordKey).getArgSetDelta().isEmpty()) {
                        for (String argToRemove : trxEntry.getRecordDelta().get(recordKey).getArgSetDelta()) {
                            record.removeFromArgSet(argToRemove);
                        }
                    }

                    if (trxEntry.getRecordDelta().get(recordKey).getArgListDelta() != null
                            && trxEntry.getRecordDelta().get(recordKey).getArgListDelta() != null
                            && !trxEntry.getRecordDelta().get(recordKey).getArgListDelta().isEmpty()) {
                        for (String argToRemove : trxEntry.getRecordDelta().get(recordKey).getArgListDelta()) {
                            record.removeFromArgList(argToRemove);
                        }
                    }
                }
            }
        }

        if (m.getLasts() != null && !m.getLasts().isEmpty()) {
            if (trxEntry != null && trxEntry.getLastsDelta() != null && !trxEntry.getLastsDelta().isEmpty()) {
                for (Entry<String, Object> entry : trxEntry.getLastsDelta().entrySet()) {
                    String lastKey = entry.getKey();
                    m.removeFromLast(lastKey);
                }
            }

        }
    }

    /**
     * Creates a map of record deltas by comparing the initial and final
     * measurements.
     *
     * @param initialMeasurment The initial measurement.
     * @param finalMeasurment   The final measurement.
     * @return A map of record deltas.
     */
    private Map<String, RecordsDelta> createRecordsDelta(Measurment initialMeasurment, Measurment finalMeasurment) {
        Map<String, RecordsDelta> deltas = new HashMap<>();
        if (initialMeasurment == null || finalMeasurment == null) {
            throw new RuntimeException("Initial or final measurement for a transaction cannot be null");
        }

        if (initialMeasurment.getRecords().isEmpty()) {
            for (Entry<String, MeasurmentRecord> entry : Collections
                    .unmodifiableMap(finalMeasurment.getRecords().getRecordHashMap()).entrySet()) {
                String recordKey = entry.getKey();
                MeasurmentRecord record = entry.getValue();

                deltas.put(recordKey, new RecordsDelta());
                deltas.get(recordKey).setCountDelta(record.getCount());
                deltas.get(recordKey).setAmountDelta(record.getAmount());
                deltas.get(recordKey).setValuesDelta(new HashMap<>(record.getValues()));
                deltas.get(recordKey).setArgListDelta(new ArrayList<>(record.getArgList()));
                deltas.get(recordKey).setArgSetDelta(new HashSet<>(record.getArgSet()));
            }

            return deltas;
        }

        for (Entry<String, MeasurmentRecord> entry : Collections
                .unmodifiableMap(finalMeasurment.getRecords().getRecordHashMap()).entrySet()) {
            String recordKey = entry.getKey();
            MeasurmentRecord finalRecord = entry.getValue();
            MeasurmentRecord initialRecord = initialMeasurment.getRecords().peek(recordKey);

            if (initialRecord == null) {
                deltas.put(recordKey, new RecordsDelta());
                deltas.get(recordKey).setCountDelta(finalRecord.getCount());
                deltas.get(recordKey).setAmountDelta(finalRecord.getAmount());
                deltas.get(recordKey).setValuesDelta(new HashMap<>(finalRecord.getValues()));
                deltas.get(recordKey).setArgListDelta(new ArrayList<>(finalRecord.getArgList()));
                deltas.get(recordKey).setArgSetDelta(new HashSet<>(finalRecord.getArgSet()));
            } else {
                deltas.put(recordKey, new RecordsDelta());
                deltas.get(recordKey).setCountDelta(
                        finalRecord.getCount() - (initialRecord.getCount() == null ? 0L : initialRecord.getCount()));
                deltas.get(recordKey).setAmountDelta(finalRecord.getAmount()
                        - (initialRecord.getAmount() == null ? 0.0 : initialRecord.getAmount()));

                Map<String, Object> valuesDelta = new HashMap<>();
                for (Entry<String, Object> valueEntry : finalRecord.getValues().entrySet()) {
                    String attrKey = valueEntry.getKey();
                    Object finalValue = valueEntry.getValue();
                    Object initialValue = initialRecord.getValues().get(attrKey);
                    if (finalValue instanceof Double) {
                        Double deltaValue = (Double) finalValue
                                - (initialValue != null && initialValue instanceof Double ? (Double) initialValue
                                        : 0.0);
                        if (deltaValue != 0.0) {
                            valuesDelta.put(attrKey, deltaValue);
                        }
                    } else if (finalValue instanceof Long) {
                        Long deltaValue = (Long) finalValue
                                - (initialValue != null && initialValue instanceof Long ? (Long) initialValue : 0L);
                        if (deltaValue != 0L) {
                            valuesDelta.put(attrKey, deltaValue);
                        }
                    } else if (finalValue instanceof Integer) {
                        Integer deltaValue = (Integer) finalValue
                                - (initialValue != null && initialValue instanceof Integer ? (Integer) initialValue
                                        : 0);
                        if (deltaValue != 0) {
                            valuesDelta.put(attrKey, deltaValue);
                        }
                    }
                }
                deltas.get(recordKey).setValuesDelta(valuesDelta);
            }

            Set<String> initialArgSet = initialRecord != null && initialRecord.getArgSet() != null
                    ? initialRecord.getArgSet()
                    : new HashSet<>();
            Set<String> finalArgSet = finalRecord != null && finalRecord.getArgSet() != null ? finalRecord.getArgSet()
                    : new HashSet<>();
            Set<String> argSetDelta = new HashSet<>(finalArgSet);
            argSetDelta.removeAll(initialArgSet);
            deltas.get(recordKey).setArgSetDelta(argSetDelta);

            List<String> initialArgList = initialRecord != null && initialRecord.getArgList() != null
                    ? initialRecord.getArgList()
                    : new ArrayList<>();
            List<String> finalArgList = finalRecord != null && finalRecord.getArgList() != null
                    ? finalRecord.getArgList()
                    : new ArrayList<>();
            List<String> argListDelta = new ArrayList<>(finalArgList);
            argListDelta.removeAll(initialArgList);
            deltas.get(recordKey).setArgListDelta(argListDelta);
        }

        return deltas;
    }

    /**
     * Creates a map of last deltas by comparing the initial and final measurements.
     *
     * @param initialMeasurment The initial measurement.
     * @param finalMeasurment   The final measurement.
     * @return A map of last deltas.
     */
    private Map<String, Object> createLastsDelta(Measurment initialMeasurment, Measurment finalMeasurment) {
        Map<String, Object> deltas = new HashMap<>();
        if (initialMeasurment == null || finalMeasurment == null) {
            throw new RuntimeException("Initial or final measurement for a transaction cannot be null");
        }

        if (initialMeasurment.getLasts().isEmpty()) {
            return new HashMap<>(finalMeasurment.getLasts());
        }

        for (Entry<String, Object> entry : Collections.unmodifiableMap(finalMeasurment.getLasts()).entrySet()) {
            String lastKey = entry.getKey();
            Object finalValue = entry.getValue();
            Object initialValue = initialMeasurment.getLasts().get(lastKey);

            if (initialValue == null || !initialValue.equals(finalValue)) {
                deltas.put(lastKey, finalValue);
            }
        }

        return deltas;
    }

    /**
     * Processes the measurement window size and updates the measurement
     * accordingly.
     *
     * @param key           The key associated with the measurement.
     * @param measurment    The measurement to process.
     * @param windowSize    The size of the window.
     * @param event         The transaction or alert event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @param subject       The subject of the event.
     * @return The updated measurement.
     */
    private Measurment processWindowSize(String key, Measurment measurment, Long windowSize, TrxOrAlertEvent event,
            Long arrivalTime, String correlationId, String subject, String customSubject) {

        VRTransactionSummary transaction = event.getTransaction();
        transaction.setTimestamp(arrivalTime);

        if (measurment == null) {
            measurment = createNewMeasument(key, subject, windowSize);
        }

        List<TrxEntry> allTrx = measurment.getTrxEntries();
        List<TrxEntry> expiredTrx = new ArrayList<>();

        for (Iterator<TrxEntry> it = allTrx.iterator(); it.hasNext();) {
            TrxEntry trxEntry = it.next();
            if (measurment.expired(trxEntry, windowSize)) {
                expiredTrx.add(trxEntry); // collect expired transaction in a temporary list
                it.remove(); // remove expired transaction from the main list
            } else {
                break;
            }
        }

        // Substract from measurment all delta generated by the expired transactions
        for (TrxEntry trxEntry : expiredTrx) {
            Long beginSubstract = System.currentTimeMillis();
            substractDelta(measurment, trxEntry);
            Long endSubstract = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [{}] win={} key={} ProcessFunction: Substract delta for expired trx {}",
                    (endSubstract - beginSubstract), correlationId, subject,
                    TimeConversion.toHumanReadableDuration(windowSize), key, trxEntry.getTxNo());
        }

        Long cloneStart = System.currentTimeMillis();
        Measurment initialMeasurment = measurment.clone();
        Long cloneEnd = System.currentTimeMillis();
        logger.debug("Time {} ms [{}] [{}] win={} key={} trx={} ProcessFunction: Cloning measurment",
                (cloneEnd - cloneStart), correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize),
                key, transaction.getTransactionNo());

        // Set the current transaction in the measurment for drools processing
        measurment.setTransaction(transaction);

        try {
            // update the indicators in the measurment (with drools)
            Long beginExecute = System.currentTimeMillis();
            executeSession(windowSize, measurment, subject + customSubject != null ? ":" + customSubject : "",
                    correlationId);
            Long endExecute = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [{}] win={} key={} ProcessFunction: executeSession() duration",
                    (endExecute - beginExecute), correlationId, subject,
                    TimeConversion.toHumanReadableDuration(windowSize), key);
        } catch (Exception e) {
            logger.error("Error inserting and executing transaction in session: {}", e.getMessage());
            e.printStackTrace();
        }

        // add the transaction in the measurment (in the current window)
        TrxEntry trxEntry = new TrxEntry();
        trxEntry.setTxNo(transaction.getTransactionNo());
        trxEntry.setTx(null);
        trxEntry.setEventTimeMs(transaction.getTimestamp());

        Long beginCreateDelta = System.currentTimeMillis();
        trxEntry.setRecordDelta(createRecordsDelta(initialMeasurment, measurment));
        trxEntry.setLastsDelta(createLastsDelta(initialMeasurment, measurment));
        Long endCreateDelta = System.currentTimeMillis();

        logger.debug("Time {} ms [{}] [{}] win={} key={} trx={} ProcessFunction: Delta creation for trx {}",
                (endCreateDelta - beginCreateDelta), correlationId, subject,
                TimeConversion.toHumanReadableDuration(windowSize), key, trxEntry.getTxNo());

        measurment.getTrxEntries().add(trxEntry);
        // Update the MapState
        measurment.setAlertSet(completeAlertSet(measurment.getAlertSet(), event, subject));
        return measurment;
    }

    /**
     * Completes the alert set by setting additional attributes based on the event
     * and subject.
     *
     * @param alertSet The alert set to complete.
     * @param event    The transaction or alert event.
     * @param subject  The subject of the event.
     * @return The completed alert set.
     */
    private AlertSet completeAlertSet(AlertSet alertSet, TrxOrAlertEvent event, String subject) {
        if (alertSet == null) {
            logger.warn("AlertSet is null, should not happen");
            return null;
        }
        alertSet.setTransactionNo(event.getTransaction().getTransactionNo());
        alertSet.setTopic(event.getTransaction().getTopic());

        if (alertSet.hasAlerts()) {
            alertSet.setAlertStatus(AlertSet.ALERT);
        } else {
            alertSet.setAlertStatus(AlertSet.NO_ALERT);
        }

        switch (subject) {
            case Subject.CARD:
                alertSet.setKey(event.getTransaction().getCardId());
                break;
            case Subject.MERCHANT:
                alertSet.setKey(event.getTransaction().getMerchant());
                break;
            case Subject.CUSTOM:
                alertSet.setKey(event.getAlertSet().getKey());
            default:
                break;
        }

        return alertSet;
    }

    /**
     * Merges two alert sets into one, combining their alerts and updating the
     * status and score.
     *
     * @param originalAlertSet The original alert set.
     * @param newAlertSet      The new alert set to merge.
     * @return The merged alert set.
     */
    private AlertSet mergeAlertSets(AlertSet originalAlertSet, AlertSet newAlertSet) {
        if (originalAlertSet == null) {
            if (newAlertSet != null) {
                newAlertSet.calculateScore(RulesConfig.alertRulesCount);
            }
            return newAlertSet;
        }
        if (newAlertSet == null) {
            if (originalAlertSet != null) {
                originalAlertSet.calculateScore(RulesConfig.alertRulesCount);
            }
            return originalAlertSet;
        }

        if (!originalAlertSet.hasAlerts()) {
            originalAlertSet.setAlerts(newAlertSet.getAlerts());
            originalAlertSet.setAlertStatus(newAlertSet.getAlertStatus());
            originalAlertSet.setScore(newAlertSet.getScore());
            return originalAlertSet;
        }
        if (!newAlertSet.hasAlerts()) {
            originalAlertSet.calculateScore(RulesConfig.alertRulesCount);
            return originalAlertSet;
        }

        for (Alert alert : newAlertSet.getAlerts()) {
            originalAlertSet.getAlerts().add(alert);
        }

        if (originalAlertSet.getAlerts().isEmpty()) {
            originalAlertSet.setAlertStatus(AlertSet.NO_ALERT);
            originalAlertSet.setScore(0.0);
        } else {
            originalAlertSet.setAlertStatus(AlertSet.ALERT);
            originalAlertSet.calculateScore(RulesConfig.alertRulesCount);
        }

        return originalAlertSet;
    }

    /**
     * Processes a message by deserializing it, processing the event, and publishing
     * a response.
     *
     * @param msg The message to process.
     */
    public void process(Message msg) {
        String topic = msg.getReplyTo();
        long arrivalTime = System.currentTimeMillis();
        try {
            // Désérialiser
            String correlationId = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-correlation-id");

            long beforeDeserialize = System.currentTimeMillis();
            @SuppressWarnings("unchecked")
            FraudCheckRequest<ITransaction> request = (FraudCheckRequest<ITransaction>) SerializationManager
                    .deserialize(msg.getData());
            long afterDeserialize = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} process(): Deserialization Time",
                    (afterDeserialize - beforeDeserialize), correlationId, Thread.currentThread().getName(),
                    request.getContent().getTransactionNo());
            VRTransactionSummary tx = new VRTransactionSummary(request.getContent());
            tx.setTopic(topic);
            tx.setTimestamp(arrivalTime);

            long recv0 = System.currentTimeMillis();
            String sClientTs = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-client-publish-ts-ms");
            String sRecvTs = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-recv-ts-ms");

            long clientTs = sClientTs != null ? Long.parseLong(sClientTs) : 0L;
            long recvTs = sRecvTs != null ? Long.parseLong(sRecvTs) : recv0;
            long publishToReceiveMs = recvTs - clientTs;
            logger.debug("Time {} ms [{}] [Thread {}] trx={} Nats: Time between API publish and fraudmanager reception",
                    publishToReceiveMs, correlationId, Thread.currentThread().getName(), tx.getTransactionNo());

            String cardKey = CARD_KEY_PREFIX + tx.getCardId();
            String merchantKey = MERCHANT_KEY_PREFIX + tx.getMerchant();
            TrxOrAlertEvent event = new TrxOrAlertEvent(tx, null, correlationId);

            // Traitement parallèle des subjects avec CompletableFuture
            CompletableFuture<TrxOrAlertEvent> cardProcessing = null;
            CompletableFuture<TrxOrAlertEvent> merchantProcessing = null;
            CompletableFuture<TrxOrAlertEvent> customProcessing = null;

            List<CompletableFuture<TrxOrAlertEvent>> cardProcessingFutures = new ArrayList<>();
            List<CompletableFuture<TrxOrAlertEvent>> merchantProcessingFutures = new ArrayList<>();

            Long endArrivalTime = System.currentTimeMillis();
            logger.debug(
                    "Time {} ms [{}] [Thread {}] trx={} process(): Pre-processing Time before starting the threads",
                    (endArrivalTime - arrivalTime), correlationId, Thread.currentThread().getName(),
                    tx.getTransactionNo());

            if (RulesConfig.cardSubjectPresent) {
                for (Map<Long, List<RuleDefinition>> ruleMapForCard : RulesConfig.rulesMapArrayForCardSubject) {
                    cardProcessing = CompletableFuture.supplyAsync(() -> {
                        try {
                            return keyProcessor.executeWithLock(cardKey, (key) -> {
                                TrxOrAlertEvent cardEvent = processEvent(event, key, arrivalTime, correlationId,
                                        Subject.CARD, null, ruleMapForCard);
                                return cardEvent;
                            });
                        } catch (Exception e) {
                            logger.error("Error processing card key: {}", cardKey, e);
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }, executor);
                    cardProcessingFutures.add(cardProcessing);
                }
            }

            if (RulesConfig.merchantSubjectPresent) {
                for (Map<Long, List<RuleDefinition>> ruleMapForMerchant : RulesConfig.rulesMapArrayForMerchantSubject) {
                    merchantProcessing = CompletableFuture.supplyAsync(() -> {
                        try {
                            return keyProcessor.executeWithLock(merchantKey, (key) -> {
                                TrxOrAlertEvent merchantEvent = processEvent(event, key, arrivalTime, correlationId,
                                        Subject.MERCHANT, null, ruleMapForMerchant);
                                return merchantEvent;
                            });
                        } catch (Exception e) {
                            logger.error("Error processing merchant key: {}", merchantKey, e);
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }, executor);
                    merchantProcessingFutures.add(merchantProcessing);
                }
            }

            List<CompletableFuture<TrxOrAlertEvent>> customProcessingFutures = new ArrayList<>();
            if (RulesConfig.customSubjectPresent) {
                for (String customSubject : RulesConfig.rulesMapForCustomSubject.keySet()) {
                    String keySpec = customSubject;
                    String keyValue = event.getTransaction().getKey(keySpec);
                    String customSubjectKey = CUSTOM_KEY_PREFIX + keyValue;
                    customProcessing = CompletableFuture.supplyAsync(() -> {
                        try {
                            return keyProcessor.executeWithLock(customSubjectKey, (key) -> {
                                TrxOrAlertEvent customEvent = processEvent(event, key, arrivalTime, correlationId,
                                        Subject.CUSTOM, customSubject,
                                        RulesConfig.rulesMapForCustomSubject.get(customSubject));
                                return customEvent;
                            });
                        } catch (Exception e) {
                            logger.error("Error processing custom subject key: {}", customSubjectKey, e);
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }, executor);
                    customProcessingFutures.add(customProcessing);
                }
            }

            // Attendre que les deux traitements se terminent et récupérer les résultats
            List<CompletableFuture<TrxOrAlertEvent>> futures = new java.util.ArrayList<>();
            if (cardProcessingFutures != null) {
                futures.addAll(cardProcessingFutures);
            }
            if (merchantProcessingFutures != null) {
                futures.addAll(merchantProcessingFutures);
            }
            if (customProcessingFutures != null && !customProcessingFutures.isEmpty()) {
                futures.addAll(customProcessingFutures);
            }

            CompletableFuture<Void> allProcessing = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allProcessing.join();
            Long endOfJoin = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} process(): Duration of threads completion",
                    endOfJoin - endArrivalTime, correlationId, Thread.currentThread().getName(), tx.getTransactionNo());

            // Récupérer les résultats
            AlertSet combinedAlertSet = new AlertSet();

            try {
                for (CompletableFuture<TrxOrAlertEvent> cp : cardProcessingFutures) {
                    TrxOrAlertEvent cardEvent = cp != null ? cp.get() : null;
                    if (cardEvent != null) {
                        combinedAlertSet = cardEvent.getAlertSet();
                    }
                }

                for (CompletableFuture<TrxOrAlertEvent> mp : merchantProcessingFutures) {
                    TrxOrAlertEvent merchantEvent = mp != null ? mp.get() : null;
                    if (merchantEvent != null) {
                        combinedAlertSet = mergeAlertSets(combinedAlertSet, merchantEvent.getAlertSet());
                    }
                }

                List<TrxOrAlertEvent> customEvents = new ArrayList<>();

                for (CompletableFuture<TrxOrAlertEvent> customFuture : customProcessingFutures) {
                    TrxOrAlertEvent customEvent = customFuture.get();
                    if (customEvent != null) {
                        customEvents.add(customEvent);
                    }
                }

                for (TrxOrAlertEvent customEvent : customEvents) {
                    combinedAlertSet = mergeAlertSets(combinedAlertSet, customEvent.getAlertSet());
                }

            } catch (Exception e) {
                logger.warn("Error retrieving processing results", e);
            }
            Long resultAgregationEnd = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} duration of results aggregation",
                    (resultAgregationEnd - endOfJoin), correlationId, Thread.currentThread().getName(),
                    tx.getTransactionNo());
            FraudCheckResponse response = new FraudCheckResponse();

            response.setAlertSet(combinedAlertSet);
            response.setCorrelationId(correlationId);
            response.setTimestamp(System.currentTimeMillis());

            // Sérialiser et publier réponse
            Long beforeSerialize = System.currentTimeMillis();
            byte[] responseBytes = SerializationManager.serialize(response);
            Long afterSerialize = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} process(): Serialization Time",
                    (afterSerialize - beforeSerialize), correlationId, Thread.currentThread().getName(),
                    tx.getTransactionNo());
            natsConnection.publish(topic, responseBytes);
        } catch (Exception e) {
            logger.error("Error processing transaction", e);
            // Ajoutez logic de retry ou dead-letter queue
        }
    }

    /**
     * Processes a card event by updating the measurement windows and merging alert
     * sets.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @return The processed card event.
     */
    private TrxOrAlertEvent processCardEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId,
            Map<Long, List<RuleDefinition>> ruleMap) {

        AlertSet alertSet = newAlertSet(event, Subject.CARD);
        TrxOrAlertEvent cardEvent = buildEvent(event, alertSet);
        String eventKey = CARD_KEY_PREFIX + key;

        for (Entry<Long, List<RuleDefinition>> entry : ruleMap.entrySet()) {
            Long ruleWindowSize = (Long) entry.getKey();
            Long stateGetStart = System.currentTimeMillis();
            Measurment measurment = rocksDBService.getMeasurmentByKey(eventKey + KEY_SEPARATOR + ruleWindowSize);
            Long stateGetEnd = System.currentTimeMillis();
            logger.debug(
                    "Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processCardEvent(): State retrieval time",
                    (stateGetEnd - stateGetStart), correlationId, Subject.CARD, Thread.currentThread().getName(),
                    TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    cardEvent.getTransaction().getTransactionNo());

            Long beforeProcessWindowSize = System.currentTimeMillis();
            measurment = processWindowSize(key, measurment, ruleWindowSize, cardEvent, arrivalTime, correlationId,
                    Subject.CARD, null);
            Long afterProcessWindowSize = System.currentTimeMillis();
            logger.debug(
                    "Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processCardEvent(): Duration of processing the window",
                    (afterProcessWindowSize - beforeProcessWindowSize), correlationId, Subject.CARD,
                    Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    cardEvent.getTransaction().getTransactionNo());

            alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
            measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
            measurment.setTransaction(null); // clear transaction to avoid serialization issues

            Long stateSetStart = System.currentTimeMillis();
            rocksDBService.setMeasurmentByKey(eventKey + KEY_SEPARATOR + ruleWindowSize, measurment);
            Long stateSetEnd = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processCardEvent(): State set time",
                    (stateSetEnd - stateSetStart), correlationId, Subject.CARD, Thread.currentThread().getName(),
                    TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    cardEvent.getTransaction().getTransactionNo());
        }

        cardEvent.setAlertSet(alertSet);
        return cardEvent;
    }

    /**
     * Processes a merchant event by updating the measurement windows and merging
     * alert sets.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @return The processed merchant event.
     */
    private TrxOrAlertEvent processMerchantEvent(TrxOrAlertEvent event, String key, Long arrivalTime,
            String correlationId, Map<Long, List<RuleDefinition>> ruleMap) {
        AlertSet alertSet = newAlertSet(event, Subject.MERCHANT);
        TrxOrAlertEvent merchantEvent = buildEvent(event, alertSet);
        String eventKey = MERCHANT_KEY_PREFIX + key;

        for (Entry<Long, List<RuleDefinition>> entry : ruleMap.entrySet()) {
            Long ruleWindowSize = (Long) entry.getKey();

            Long stateGetStart = System.currentTimeMillis();
            Measurment measurment = rocksDBService.getMeasurmentByKey(eventKey + KEY_SEPARATOR + ruleWindowSize);
            Long stateGetEnd = System.currentTimeMillis();
            logger.debug(
                    "Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processMerchantEvent(): State retrieval time",
                    (stateGetEnd - stateGetStart), correlationId, Subject.MERCHANT, Thread.currentThread().getName(),
                    TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    event.getTransaction().getTransactionNo());

            if (measurment == null) {
                measurment = createNewMeasument(key, Subject.MERCHANT, ruleWindowSize);
            }

            Long beforeProcessWindowSize = System.currentTimeMillis();
            measurment = processWindowSize(key, measurment, ruleWindowSize, merchantEvent, arrivalTime, correlationId,
                    Subject.MERCHANT, null);
            Long afterProcessWindowSize = System.currentTimeMillis();
            logger.debug(
                    "Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processMerchantEvent: Duration of processWindowSize",
                    (afterProcessWindowSize - beforeProcessWindowSize), correlationId, Subject.MERCHANT,
                    Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    merchantEvent.getTransaction().getTransactionNo());

            alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
            measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
            measurment.setTransaction(null); // clear transaction to avoid serialization issues

            Long stateSetStart = System.currentTimeMillis();
            rocksDBService.setMeasurmentByKey(eventKey + KEY_SEPARATOR + ruleWindowSize, measurment);
            Long stateSetEnd = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processMerchantEvent: State set time",
                    (stateSetEnd - stateSetStart), correlationId, Subject.MERCHANT, Thread.currentThread().getName(),
                    TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    merchantEvent.getTransaction().getTransactionNo());
        }

        merchantEvent.setAlertSet(alertSet);
        return merchantEvent;
    }

    /**
     * Processes a custom event by updating the measurement windows and merging
     * alert sets.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @return The processed custom event.
     */
    private TrxOrAlertEvent processCustomEvent(TrxOrAlertEvent event, String key, String customSubject,
            Long arrivalTime, String correlationId) {
        AlertSet alertSet = newAlertSet(event, Subject.ANY);
        TrxOrAlertEvent customEvent = buildEvent(event, alertSet);
        String eventKey = CUSTOM_KEY_PREFIX + key;

        Map<Long, List<RuleDefinition>> rulesMapForCustomSubject = RulesConfig.rulesMapForCustomSubject
                .get(customSubject);

        for (Entry<Long, List<RuleDefinition>> entry : rulesMapForCustomSubject.entrySet()) {
            Long ruleWindowSize = (Long) entry.getKey();

            Long stateGetStart = System.currentTimeMillis();
            Measurment measurment = rocksDBService.getMeasurmentByKey(eventKey + KEY_SEPARATOR + ruleWindowSize);
            Long stateGetEnd = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processCustomEvent: State get time",
                    (stateGetEnd - stateGetStart), correlationId, Subject.CUSTOM + ":" + customSubject,
                    Thread.currentThread().getName(),
                    TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    customEvent.getTransaction().getTransactionNo());

            if (measurment == null) {
                measurment = createNewMeasument(key, Subject.CUSTOM, ruleWindowSize);
            }

            Long beforeProcessWindowSize = System.currentTimeMillis();
            measurment = processWindowSize(key, measurment, ruleWindowSize, customEvent, arrivalTime, correlationId,
                    Subject.CUSTOM, customSubject);
            Long afterProcessWindowSize = System.currentTimeMillis();
            logger.debug(
                    "Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processCustomEvent: Duration of processWindowSize",
                    (afterProcessWindowSize - beforeProcessWindowSize), correlationId,
                    Subject.CUSTOM + ":" + customSubject,
                    Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key,
                    customEvent.getTransaction().getTransactionNo());

            alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
            measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
            measurment.setTransaction(null); // clear transaction to avoid serialization issues
            rocksDBService.setMeasurmentByKey(eventKey + KEY_SEPARATOR + measurment.getWindowSize(), measurment);
        }

        customEvent.setAlertSet(alertSet);
        return customEvent;
    }

    /**
     * Processes an event based on its subject type.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @param subject       The subject of the event.
     * @return The processed event.
     */
    private TrxOrAlertEvent processEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId,
            String subject, String customSubject, Map<Long, List<RuleDefinition>> ruleMap) {
        switch (subject) {
            case Subject.CARD:
                return processCardEvent(event, key, arrivalTime, correlationId, ruleMap);
            case Subject.MERCHANT:
                return processMerchantEvent(event, key, arrivalTime, correlationId, ruleMap);
            case Subject.CUSTOM:
                return processCustomEvent(event, key, customSubject, arrivalTime, correlationId);
            default:
                logger.error("Unknown subject type: {}", subject);
                return event;
        }
    }

    /**
     * Fermeture propre du processor et de ses ressources
     */
    public void shutdown() {

        if (executor != null && !executor.isShutdown()) {
            logger.info("Shutting down FraudProcessor executor...");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    logger.warn("Executor did not terminate gracefully, forcing shutdown...");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for executor termination", e);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        sessionPools.values().forEach(queue -> {
            queue.forEach(session -> {
                try {
                    session.dispose();
                } catch (Exception ignore) {
                }
            });
            queue.clear();
        });
        sessionPools.clear();
        logger.info("FraudProcessor shut down.");

    }

}
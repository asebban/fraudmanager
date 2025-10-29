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
import ma.s2m.fraudmanager.drools.listeners.RuleProfiler;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudProcessor {

    private static final String CARD_KEY_PREFIX = Subject.CARD + ":";
    private static final String MERCHANT_KEY_PREFIX = Subject.MERCHANT + ":";
    private static final String ANY_KEY_PREFIX = Subject.ANY + ":";
    private static final String CUSTOM_KEY_PREFIX = Subject.CUSTOM + ":";

    private static final Logger logger = LoggerFactory.getLogger(FraudProcessor.class);
    private final RedisService redisService;
    private final Connection natsConnection;
    private final KeyProcessor keyProcessor;
    private final ExecutorService executor = Executors.newFixedThreadPool(4); // Pool pour traitement parallèle
    private IMessageSender messageSender;
    private DroolsSessionFactory sessionFactory;
    private final Set<DroolsSession> allSessions = ConcurrentHashMap.newKeySet();
    private final ThreadLocal<DroolsSession> threadLocalSession = ThreadLocal.withInitial(() -> {
        DroolsSession s = createNewDroolsSession();   // méthode ci-dessous
        allSessions.add(s);
        // warm-up léger par thread (optionnel mais recommandé)
        try { warmUpDrools(Subject.CARD, s); } catch (Exception ignore) {}
        return s;
    });

    private DroolsSession createNewDroolsSession() {
        HashMap<String, Object> globals = new HashMap<>();
        globals.put("timeConverter", new TimeConversion());
        globals.put("externalSystem", new ExternalSystem());
        globals.put("messageSender", messageSender);
        globals.put("typeConverter", new TypeConverter());

        DroolsSession s = sessionFactory.newSession(SessionMode.STATEFUL, globals);
        if (AppConfig.droolsProfilerEnabled) {
            s.addEventListener(new RuleProfiler());
        }
        return s;
    }

    public FraudProcessor(RedisService redisService, Connection natsConnection) throws IOException {
        this.redisService = redisService;
        this.natsConnection = natsConnection;
        this.keyProcessor = new KeyProcessor(redisService);
        initProcessor();
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
                this.messageSender = (IMessageSender) Class.forName(properties.getProperty("app.processor.messaging.provider", "ma.medtech.droolbuilder.messaging.SysoutMessageSender")).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException("ServiceFactory: Error while creating message sender", e);
            }
            
            this.sessionFactory = new DroolsSessionFactory(RulesConfig.extendedVersion);
    }

    private DroolsSession executeSession(Long windowSize, Measurment measurment, String subject) throws Exception {
        return executeSession(windowSize, measurment, subject, this.threadLocalSession.get());
    }

    private DroolsSession executeSession(Long windowSize, Measurment measurment, String subject, DroolsSession s) throws Exception {
        if (measurment == null) {
            throw new IllegalArgumentException("executeSession: Measurment cannot be null");
        }
        if (measurment.getAlertSet() == null) {
            measurment.setAlertSet(new AlertSet());
        }
        s.execute(measurment, windowSize, subject);
        return s;
    }

    private void warmUpDrools(String subject, DroolsSession s) {
        VirtualRecordTransaction dummyTrx = TransactionDummyHelper.dummyTransaction();
        Measurment m = createNewMeasument("card-1", subject, 10000L);
        m.setTransaction(new VRTransactionSummary(dummyTrx));
        try {
            // on warm-up la session fournie
            executeSession(10000L, m, subject, s);
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
        alertSet.setSubject(subject);
        switch(subject) {
            case Subject.CARD:
                alertSet.setCardId(event.getTransaction().getCardId());
                break;
            case Subject.MERCHANT:
                alertSet.setMerchantId(event.getTransaction().getMerchant());
                break;
            case Subject.ANY:
                break;
            default:
                logger.warn("### [{}] ProcessFunction Unknown subject type: {} for transaction: {}", event.getCorrelationId(), subject, event.getTransaction().getTransactionNo());
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

        if (alertSet == null || alertSet.getAlerts() == null || alertSet.getAlerts().isEmpty() || alertSet.getAlertStatus().equals(AlertSet.NO_ALERT)) {
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
     * Subtracts a double value from the specified attribute in the measurement record.
     *
     * @param entry   The entry containing the attribute key and value.
     * @param record  The measurement record to update.
     * @param attrKey The key of the attribute to subtract the value from.
     */
    private void substractDoubleValue(Entry<String, Object> entry, MeasurmentRecord record, String attrKey) {
        if (entry.getValue() != null && entry.getValue() instanceof Double && (Double)entry.getValue() != 0.0) {
            Double deltaValue = (Double) entry.getValue();
            if (record.getValues().containsKey(attrKey) && record.getValues().get(attrKey) instanceof Double) {
                Double currentValue = (Double) record.getValues().get(attrKey);
                record.getValues().put(attrKey, currentValue - deltaValue);
            }
        }
    }

    /**
     * Subtracts an integer value from the specified attribute in the measurement record.
     *
     * @param entry   The entry containing the attribute key and value.
     * @param record  The measurement record to update.
     * @param attrKey The key of the attribute to subtract the value from.
     */
    private void substractIntegerValue(Entry<String, Object> entry, MeasurmentRecord record, String attrKey) {
        if (entry.getValue() != null && entry.getValue() instanceof Integer && (Integer)entry.getValue() != 0) {
            Integer deltaValue = (Integer) entry.getValue();
            if (record.getValues().containsKey(attrKey) && record.getValues().get(attrKey) instanceof Integer) {
                Integer currentValue = (Integer) record.getValues().get(attrKey);
                record.getValues().put(attrKey, currentValue - deltaValue);
            }
        }
    }

    /**
     * Subtracts a long value from the specified attribute in the measurement record.
     *
     * @param entry   The entry containing the attribute key and value.
     * @param record  The measurement record to update.
     * @param attrKey The key of the attribute to subtract the value from.
     */
    private void substractLongValue(Entry<String, Object> entry, MeasurmentRecord record, String attrKey) {
        if (entry.getValue() != null && entry.getValue() instanceof Long && (Long)entry.getValue() != 0L) {
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

                    for(Entry<String, Object> entry: trxEntry.getRecordDelta().get(recordKey).getValuesDelta().entrySet()) {
                        String attrKey = entry.getKey();
                        substractDoubleValue(entry, record, attrKey);
                        substractLongValue(entry, record, attrKey);
                        substractIntegerValue(entry, record, attrKey);
                    }

                    if (trxEntry.getRecordDelta().get(recordKey).getArgSetDelta() != null && trxEntry.getRecordDelta().get(recordKey).getArgSetDelta() != null && !trxEntry.getRecordDelta().get(recordKey).getArgSetDelta().isEmpty()) {
                        for (String argToRemove : trxEntry.getRecordDelta().get(recordKey).getArgSetDelta()) {
                            record.removeFromArgList(argToRemove);
                        }
                    }

                    if (trxEntry.getRecordDelta().get(recordKey).getArgListDelta() != null && trxEntry.getRecordDelta().get(recordKey).getArgListDelta() != null && !trxEntry.getRecordDelta().get(recordKey).getArgListDelta().isEmpty()) {
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
     * Creates a map of record deltas by comparing the initial and final measurements.
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
            for(Entry<String, MeasurmentRecord> entry : Collections.unmodifiableMap(finalMeasurment.getRecords().getRecordHashMap()).entrySet()) {
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

        for(Entry<String, MeasurmentRecord> entry : Collections.unmodifiableMap(finalMeasurment.getRecords().getRecordHashMap()).entrySet()) {
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
            }
            else {
                deltas.put(recordKey, new RecordsDelta());
                deltas.get(recordKey).setCountDelta(finalRecord.getCount() - (initialRecord.getCount() == null ? 0L : initialRecord.getCount()));
                deltas.get(recordKey).setAmountDelta(finalRecord.getAmount() - (initialRecord.getAmount() == null ? 0.0 : initialRecord.getAmount()));

                Map<String, Object> valuesDelta = new HashMap<>();
                for (Entry<String, Object> valueEntry : finalRecord.getValues().entrySet()) {
                    String attrKey = valueEntry.getKey();
                    Object finalValue = valueEntry.getValue();
                    Object initialValue = initialRecord.getValues().get(attrKey);
                    if (finalValue instanceof Double) {
                        Double deltaValue = (Double)finalValue - (initialValue != null && initialValue instanceof Double ? (Double)initialValue : 0.0);
                        if (deltaValue != 0.0) {
                            valuesDelta.put(attrKey, deltaValue);
                        }
                    }
                    else if (finalValue instanceof Long) {
                        Long deltaValue = (Long)finalValue - (initialValue != null && initialValue instanceof Long ? (Long)initialValue : 0L);
                        if (deltaValue != 0L) {
                            valuesDelta.put(attrKey, deltaValue);
                        }
                    }
                    else if (finalValue instanceof Integer) {
                        Integer deltaValue = (Integer)finalValue - (initialValue != null && initialValue instanceof Integer ? (Integer)initialValue : 0);
                        if (deltaValue != 0) {
                            valuesDelta.put(attrKey, deltaValue);
                        }   
                    }
                }
                deltas.get(recordKey).setValuesDelta(valuesDelta);
            }

            Set<String> initialArgSet = initialRecord != null && initialRecord.getArgSet() != null ? initialRecord.getArgSet() : new HashSet<>();
            Set<String> finalArgSet = finalRecord != null && finalRecord.getArgSet() != null ? finalRecord.getArgSet() : new HashSet<>();
            Set<String> argSetDelta = new HashSet<>(finalArgSet);
            argSetDelta.removeAll(initialArgSet);
            deltas.get(recordKey).setArgSetDelta(argSetDelta);

            List<String> initialArgList = initialRecord != null && initialRecord.getArgList() != null ? initialRecord.getArgList() : new ArrayList<>();
            List<String> finalArgList = finalRecord != null && finalRecord.getArgList() != null ? finalRecord.getArgList() : new ArrayList<>();
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

        for(Entry<String, Object> entry : Collections.unmodifiableMap(finalMeasurment.getLasts()).entrySet()) {
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
     * Processes the measurement window size and updates the measurement accordingly.
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
    private Measurment processWindowSize(String key, Measurment measurment, Long windowSize, TrxOrAlertEvent event, Long arrivalTime, String correlationId, String subject) {

        VRTransactionSummary transaction = event.getTransaction();
        transaction.setTransmissionDateTime(arrivalTime);
        Long startTime = System.currentTimeMillis();

        if (measurment == null) {
            measurment = createNewMeasument(key, subject, windowSize);
        }

        logger.debug("### [{}] [{}] win={} key={} ProcessWindowSize: Start processing trx {} and logging measurment before processing",
                correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, transaction.getTransactionNo());
        logMeasurment(measurment, windowSize, subject, transaction.getTransactionNo());

        int mainSizeBefore = measurment.getTrxEntries().size();

        List<TrxEntry> allTrx = measurment.getTrxEntries();
        List<TrxEntry> expiredTrx = new ArrayList<>();

        for (Iterator<TrxEntry> it = allTrx.iterator(); it.hasNext(); ) {
            TrxEntry trxEntry = it.next();
            if (measurment.expired(trxEntry, windowSize)) {
                expiredTrx.add(trxEntry);  // collect expired transaction in a temporary list
                it.remove();     // remove expired transaction from the main list
            }
            else {
                break;
            }
        }

        int expiredCount = expiredTrx.size();

        // Substract from measurment all delta generated by the expired transactions
        for (TrxEntry trxEntry : expiredTrx) {
            Long beginSubstract = System.currentTimeMillis();
            substractDelta(measurment, trxEntry);
            Long endSubstract = System.currentTimeMillis();
            logger.debug("### [{}] [{}] win={} key={} ProcessFunction: Substract delta for expired trx {} took {} ms",
                    correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, trxEntry.getTxNo(), (endSubstract - beginSubstract));
        }

        Long cloneStart = System.currentTimeMillis();
        Measurment initialMeasurment = measurment.clone();
        Long cloneEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] win={} key={} ProcessFunction: Cloning measurment took {} ms",
                correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, (cloneEnd - cloneStart));

        // Set the current transaction in the measurment for drools processing
        measurment.setTransaction(transaction);

        long t0 = System.nanoTime();
        Long tend = t0;
        try {
            // update the indicators in the measurment (with drools)
            executeSession(windowSize, measurment, subject);
        } catch (Exception e) {
            logger.error("Error inserting and executing transaction in session: {}", e.getMessage());
            e.printStackTrace();
        }
        finally {
            tend = System.nanoTime();
            logger.debug("### [{}] [{}] win={} key={} ProcessFunction: Drools execution time for trx {} was {} ms",
                    correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, measurment.getTransaction().getTransactionNo(), (tend - t0) / 1_000_000L);
        }

        logger.debug("### [{}] [{}] win={} key={} ProcessFunction: Logging measurment after processing trx {}",
                correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, transaction.getTransactionNo());
        logMeasurment(measurment, windowSize, subject, transaction.getTransactionNo());

        // add the transaction in the measurment (in the current window)
        TrxEntry trxEntry = new TrxEntry();
        trxEntry.setTxNo(transaction.getTransactionNo());
        trxEntry.setTx(null);
        trxEntry.setEventTimeMs(transaction.getTransmissionDateTime());

        Long beginCreateDelta = System.currentTimeMillis();
        trxEntry.setRecordDelta(createRecordsDelta(initialMeasurment, measurment));
        trxEntry.setLastsDelta(createLastsDelta(initialMeasurment, measurment));
        Long endCreateDelta = System.currentTimeMillis();

        logger.debug("### [{}] [{}] win={} key={} ProcessFunction: Delta creation for trx {} took {} ms",
                correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, trxEntry.getTxNo(), (endCreateDelta - beginCreateDelta));

        measurment.getTrxEntries().add(trxEntry);
        int mainSizeAfter = measurment.getTrxEntries().size();
        logger.debug("### [{}] [{}] win={} key={} ProcessFunction: mainSizeBefore={} expiredCount={} mainSizeAfter={}",
                correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, mainSizeBefore, expiredCount, mainSizeAfter);

        Long endTime = System.currentTimeMillis();
        logger.debug("### [{}] [{}] win={} key={} ProcessFunction: Total processing time for trx {} took {} ms",
                correlationId, subject, TimeConversion.toHumanReadableDuration(windowSize), key, transaction.getTransactionNo(), endTime - startTime);

        // Update the MapState
        measurment.setAlertSet(completeAlertSet(measurment.getAlertSet(), event, subject));
        return measurment;      
    }

    /**
     * Completes the alert set by setting additional attributes based on the event and subject.
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
        alertSet.setSubject(subject);

        if (alertSet.hasAlerts()) {
            alertSet.setAlertStatus(AlertSet.ALERT);
        } else {
            alertSet.setAlertStatus(AlertSet.NO_ALERT);
        }

        switch(subject) {
            case Subject.CARD:
                alertSet.setCardId(event.getTransaction().getCardId());
                break;
            case Subject.MERCHANT:
                alertSet.setMerchantId(event.getTransaction().getMerchant());
                break;
            default:
                break;
        }

        return alertSet;
    }

    /**
     * Merges two alert sets into one, combining their alerts and updating the status and score.
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
     * Processes a message by deserializing it, processing the event, and publishing a response.
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
            FraudCheckRequest<ITransaction> request = (FraudCheckRequest<ITransaction>) SerializationManager.deserialize(msg.getData());
            long afterDeserialize = System.currentTimeMillis();
            logger.debug("[{}] process(): Deserialization for trx {} took {} ms", correlationId, request.getContent().getTransactionNo(), (afterDeserialize - beforeDeserialize));
            VRTransactionSummary tx = new VRTransactionSummary(request.getContent());
            tx.setTopic(topic);
            tx.setTransmissionDateTime(arrivalTime);

            long recv0 = System.currentTimeMillis();         
            String sClientTs     = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-client-publish-ts-ms");
            String sRecvTs       = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-recv-ts-ms");

            long clientTs = sClientTs != null ? Long.parseLong(sClientTs) : 0L;
            long recvTs   = sRecvTs   != null ? Long.parseLong(sRecvTs)   : recv0;
            long publishToReceiveMs = recvTs - clientTs;
            logger.debug("### [{}] Nats: Time between API publish and fraudmanager reception is {} ms", correlationId, publishToReceiveMs);

            String cardKey = CARD_KEY_PREFIX + tx.getCardId();
            String merchantKey = MERCHANT_KEY_PREFIX + tx.getMerchant();
            TrxOrAlertEvent event = new TrxOrAlertEvent(tx, null, correlationId);

            // Traitement parallèle des subjects avec CompletableFuture
            CompletableFuture<TrxOrAlertEvent> cardProcessing = null;
            CompletableFuture<TrxOrAlertEvent> merchantProcessing = null;
            CompletableFuture<TrxOrAlertEvent> anyProcessing = null;
            CompletableFuture<TrxOrAlertEvent> customProcessing = null;

            if (RulesConfig.cardSubjectPresent) {
                cardProcessing = CompletableFuture.supplyAsync(() -> {
                    try {
                        return keyProcessor.executeWithLock(cardKey, (key) -> {
                            return processEvent(event, key, arrivalTime, correlationId, Subject.CARD);
                        });
                    } catch (Exception e) {
                        logger.error("Error processing card key: {}", cardKey, e);
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }, executor);
            }

            if (RulesConfig.merchantSubjectPresent) {
                merchantProcessing = CompletableFuture.supplyAsync(() -> {
                    try {
                        return keyProcessor.executeWithLock(merchantKey, (key) -> {
                            return processEvent(event, key, arrivalTime, correlationId, Subject.MERCHANT);
                        });
                    } catch (Exception e) {
                        logger.error("Error processing merchant key: {}", merchantKey, e);
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }, executor);
            }

            List<CompletableFuture<TrxOrAlertEvent>> customProcessings = new ArrayList<>();
            if (RulesConfig.customSubjectPresent) {
                for (String customSubject : RulesConfig.rulesMapForCustomSubject.keySet()) {
                    customProcessing = CompletableFuture.supplyAsync(() -> {
                        try {
                            String customKey = CUSTOM_KEY_PREFIX + customSubject;
                            return keyProcessor.executeWithLock(customKey, (key) -> {
                                return processEvent(event, key, arrivalTime, correlationId, Subject.CUSTOM);
                            });
                        } catch (Exception e) {
                            logger.error("Error processing custom subject key: {}", customSubject, e);
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }, executor);
                    customProcessings.add(customProcessing);
                }
            }

            if (RulesConfig.anySubjectPresent) {
                String anyKey = ANY_KEY_PREFIX + "global";
                anyProcessing = CompletableFuture.supplyAsync(() -> {
                    try {
                        return keyProcessor.executeWithLock(anyKey, (key) -> {
                            return processEvent(event, key, arrivalTime, correlationId, Subject.ANY);
                        });
                    } catch (Exception e) {
                        logger.error("Error processing any key: {}", anyKey, e);
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }, executor);
            }

            // Attendre que les deux traitements se terminent et récupérer les résultats
            List<CompletableFuture<TrxOrAlertEvent>> futures = new java.util.ArrayList<>();
            if (cardProcessing != null) {
                futures.add(cardProcessing);
            }
            if (merchantProcessing != null) {
                futures.add(merchantProcessing);
            }
            if (anyProcessing != null) {
                futures.add(anyProcessing);
            }
            if (customProcessings != null && !customProcessings.isEmpty()) {
                futures.addAll(customProcessings);
            }

            
            CompletableFuture<Void> allProcessing = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allProcessing.join();
            
            // Récupérer les résultats
            AlertSet combinedAlertSet = new AlertSet();

            try {
                TrxOrAlertEvent cardEvent = cardProcessing != null ? cardProcessing.get() : null;
                TrxOrAlertEvent merchantEvent = merchantProcessing != null ? merchantProcessing.get() : null;
                TrxOrAlertEvent anyEvent = anyProcessing != null ? anyProcessing.get() : null;
                List<TrxOrAlertEvent> customEvents = new ArrayList<>();

                if (cardEvent != null) {
                    combinedAlertSet = cardEvent.getAlertSet();
                }
                if (merchantEvent != null) {
                    combinedAlertSet = mergeAlertSets(combinedAlertSet, merchantEvent.getAlertSet());
                }
                if (anyEvent != null) {
                    combinedAlertSet = mergeAlertSets(combinedAlertSet, anyEvent.getAlertSet());
                }

                for (CompletableFuture<TrxOrAlertEvent> customFuture : customProcessings) {
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
            
            FraudCheckResponse response = new FraudCheckResponse();

            response.setAlertSet(combinedAlertSet);
            response.setCorrelationId(correlationId);
            response.setTimestamp(System.currentTimeMillis());

            // Sérialiser et publier réponse
            byte[] responseBytes = SerializationManager.serialize(response);
            natsConnection.publish(topic, responseBytes);

            long duration = System.currentTimeMillis() - arrivalTime;
            if (duration > 200) {
                logger.warn("[{}] Transaction {} took {} ms > 200 ms", correlationId, event.getTransaction().getTransactionNo(), duration);
            }
        } catch (Exception e) {
            logger.error("Error processing transaction", e);
            // Ajoutez logic de retry ou dead-letter queue
        }
    }

    /**
     * Processes a card event by updating the measurement windows and merging alert sets.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @return The processed card event.
     */
    private TrxOrAlertEvent processCardEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId) {
        long windowsStart = System.currentTimeMillis();
        AlertSet alertSet = newAlertSet(event, Subject.CARD);
        TrxOrAlertEvent cardEvent = buildEvent(event, alertSet);
        String eventKey = CARD_KEY_PREFIX + key;
        Map<Long, Measurment> windows = redisService.getMeasurments(eventKey);

        for (Entry<Long, List<RuleDefinition>> entry : RulesConfig.rulesMapForCardSubject.entrySet()) {

            Long ruleWindowSize = (Long) entry.getKey();
            long retrieveBegin = System.currentTimeMillis();
            Measurment measurment = windows.get(ruleWindowSize);
            long retrieveEnd = System.currentTimeMillis();
            logger.debug("### [{}] [{}] [Thread : {}] win={} key={} ProcessFunction: Retrieving state took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key, (retrieveEnd - retrieveBegin));

            measurment = processWindowSize(key, measurment, ruleWindowSize, cardEvent, arrivalTime, correlationId, Subject.CARD);

            alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
            measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
            measurment.setTransaction(null); // clear transaction to avoid serialization issues

            windows.put(ruleWindowSize, measurment);
        }

        long updateBegin = System.currentTimeMillis();
        redisService.setMeasurments(eventKey, windows);
        long updateEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: Updating state took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (updateEnd - updateBegin));

        long windowsEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: All windows processing took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (windowsEnd - windowsStart));
        cardEvent.setAlertSet(alertSet);
        return cardEvent;    
    }

    /**
     * Processes a merchant event by updating the measurement windows and merging alert sets.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @return The processed merchant event.
     */
    private TrxOrAlertEvent processMerchantEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId) {
        long windowsStart = System.currentTimeMillis();
        AlertSet alertSet = newAlertSet(event, Subject.CARD);
        TrxOrAlertEvent merchantEvent = buildEvent(event, alertSet);
        String eventKey = MERCHANT_KEY_PREFIX + key;
        Map<Long, Measurment> windows = redisService.getMeasurments(eventKey);

        for (Entry<Long, List<RuleDefinition>> entry : RulesConfig.rulesMapForMerchantSubject.entrySet()) {

            Long ruleWindowSize = (Long) entry.getKey();
            long retrieveBegin = System.currentTimeMillis();
            Measurment measurment = windows.get(ruleWindowSize);
            long retrieveEnd = System.currentTimeMillis();
            logger.debug("### [{}] [{}] [Thread : {}] win={} key={} ProcessFunction: Retrieving state took {} ms", correlationId, Subject.MERCHANT, Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key, (retrieveEnd - retrieveBegin));

            measurment = processWindowSize(key, measurment, ruleWindowSize, merchantEvent, arrivalTime, correlationId, Subject.MERCHANT);

            alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
            measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
            measurment.setTransaction(null); // clear transaction to avoid serialization issues

            windows.put(ruleWindowSize, measurment);
        }

        long updateBegin = System.currentTimeMillis();
        redisService.setMeasurments(eventKey, windows);
        long updateEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: Updating state took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (updateEnd - updateBegin));

        long windowsEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: All windows processing took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (windowsEnd - windowsStart));
        merchantEvent.setAlertSet(alertSet);
        return merchantEvent;    
    }

    /**
     * Processes an "any" event by updating the measurement windows and merging alert sets.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @return The processed "any" event.
     */
    private TrxOrAlertEvent processAnyEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId) {
        long windowsStart = System.currentTimeMillis();
        AlertSet alertSet = newAlertSet(event, Subject.ANY);
        TrxOrAlertEvent anyEvent = buildEvent(event, alertSet);
        String eventKey = ANY_KEY_PREFIX + key;
        Map<Long, Measurment> windows = redisService.getMeasurments(eventKey);

        for (Entry<Long, List<RuleDefinition>> entry : RulesConfig.rulesMapForAnySubject.entrySet()) {

            Long ruleWindowSize = (Long) entry.getKey();
            long retrieveBegin = System.currentTimeMillis();
            Measurment measurment = windows.get(ruleWindowSize);
            long retrieveEnd = System.currentTimeMillis();
            logger.debug("### [{}] [{}] [Thread : {}] win={} key={} ProcessFunction: Retrieving state took {} ms", correlationId, Subject.MERCHANT, Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key, (retrieveEnd - retrieveBegin));

            measurment = processWindowSize(key, measurment, ruleWindowSize, anyEvent, arrivalTime, correlationId, Subject.ANY);

            alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
            measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
            measurment.setTransaction(null); // clear transaction to avoid serialization issues

            windows.put(ruleWindowSize, measurment);
        }

        long updateBegin = System.currentTimeMillis();
        redisService.setMeasurments(eventKey, windows);
        long updateEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: Updating state took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (updateEnd - updateBegin));

        long windowsEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: All windows processing took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (windowsEnd - windowsStart));
        anyEvent.setAlertSet(alertSet);
        return anyEvent;    
    }

    /**
     * Processes a custom event by updating the measurement windows and merging alert sets.
     *
     * @param event         The transaction or alert event.
     * @param key           The key associated with the event.
     * @param arrivalTime   The arrival time of the event.
     * @param correlationId The correlation ID for logging.
     * @return The processed custom event.
     */
    private TrxOrAlertEvent processCustomEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId) {
        long windowsStart = System.currentTimeMillis();
        AlertSet alertSet = newAlertSet(event, Subject.ANY);
        TrxOrAlertEvent customEvent = buildEvent(event, alertSet);
        String customSubject = key;
        VRTransactionSummary tx = event.getTransaction();
        
        String cleanedCustomSubject = VRTransactionSummary.cleanCustomSubject(customSubject);

        try {
            key = tx.getKey(key);
            key = cleanedCustomSubject + "/" + key;
        } catch (IllegalArgumentException | NoSuchFieldException e) {
            e.printStackTrace();
        }

        String eventKey = CUSTOM_KEY_PREFIX + key;
        Map<Long, Measurment> windows = redisService.getMeasurments(eventKey);

        Map<Long, List<RuleDefinition>> rulesMapForCustomSubject = RulesConfig.rulesMapForCustomSubject.get(customSubject);

        for (Entry<Long, List<RuleDefinition>> entry : rulesMapForCustomSubject.entrySet()) {

            Long ruleWindowSize = (Long) entry.getKey();
            long retrieveBegin = System.currentTimeMillis();
            Measurment measurment = windows.get(ruleWindowSize);
            long retrieveEnd = System.currentTimeMillis();
            logger.debug("### [{}] [{}] [Thread : {}] win={} key={} ProcessFunction: Retrieving state took {} ms", correlationId, Subject.CUSTOM, Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key, (retrieveEnd - retrieveBegin));

            measurment = processWindowSize(key, measurment, ruleWindowSize, customEvent, arrivalTime, correlationId, Subject.CUSTOM);

            alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
            measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
            measurment.setTransaction(null); // clear transaction to avoid serialization issues

            windows.put(ruleWindowSize, measurment);
        }

        long updateBegin = System.currentTimeMillis();
        redisService.setMeasurments(eventKey, windows);
        long updateEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: Updating state took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (updateEnd - updateBegin));

        long windowsEnd = System.currentTimeMillis();
        logger.debug("### [{}] [{}] [Thread : {}] key={} ProcessFunction: All windows processing took {} ms", correlationId, Subject.CARD, Thread.currentThread().getName(), key, (windowsEnd - windowsStart));
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
    private TrxOrAlertEvent processEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId, String subject) {
        switch(subject) {
            case Subject.CARD:
                return processCardEvent(event, key, arrivalTime, correlationId);
            case Subject.MERCHANT:
                return processMerchantEvent(event, key, arrivalTime, correlationId);
            case Subject.ANY:
                return processAnyEvent(event, key, arrivalTime, correlationId);
            case Subject.CUSTOM:
                return processCustomEvent(event, key, arrivalTime, correlationId);
            default:
                logger.error("Unknown subject type: {}", subject);
                return null;
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

        try {
            for (DroolsSession s : allSessions) {
                try { s.dispose(); } catch (Exception ignore) {}
            }
        } finally {
            allSessions.clear();
            threadLocalSession.remove();
        }

    }

    /**
     * Logs the measurement details for debugging purposes.
     *
     * @param m          The measurement to log.
     * @param windowSize The size of the window.
     * @param subject    The subject of the measurement.
     * @param trxNo      The transaction number.
     */
    private void logMeasurment(Measurment m, Long windowSize, String subject, String trxNo) {
        if (m == null) {
            logger.debug("Measurment is null for trx {} subject {} window {}", trxNo, subject, TimeConversion.toHumanReadableDuration(windowSize));
            return;
        }
        logger.debug("Measurment for trx {} subject {} window {}: {}", trxNo, subject, TimeConversion.toHumanReadableDuration(windowSize), m.toString());
    }
}
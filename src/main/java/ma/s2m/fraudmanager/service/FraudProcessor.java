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
import ma.s2m.fraudmanager.model.WrapperMeasurment;
import ma.s2m.fraudmanager.util.Subject;
import ma.s2m.serializer.SerializationManager;
import io.nats.client.Connection;
import io.nats.client.Message;
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

    public static final String WINDOW_SEPARATOR = "/";
    public static final String KEY_SEPARATOR = ":";
    public static final String FIXED_WINDOW_PREFIX = "FW:";
    public static final String CARD_KEY_PREFIX = Subject.CARD + KEY_SEPARATOR;
    public static final String MERCHANT_KEY_PREFIX = Subject.MERCHANT + KEY_SEPARATOR;
    public static final String CUSTOM_KEY_PREFIX = Subject.CUSTOM + KEY_SEPARATOR;
    public static final String NO_ERROR_MESSAGE = "";
    public static final Integer FIXED_WINDOW = 1;
    public static final Integer SLIDING_WINDOW = 2;

    private static final Logger logger = LoggerFactory.getLogger(FraudProcessor.class);
    private final RocksDBService rocksDBService;
    private final Connection natsConnection;
    private final KeyProcessor keyProcessor;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor(); // Pool pour traitement
                                                                                          // parallèle
    private IMessageSender messageSender;
    private DroolsSessionFactory sessionFactory;
    private int sessionPoolSize = AppConfig.appThreadSessionPoolSize;
    private List<BlockingQueue<SubjectTask>> cardQueues = new ArrayList<>();
    private List<BlockingQueue<SubjectTask>> merchantQueues = new ArrayList<>();
    private Map<String, BlockingQueue<SubjectTask>> customQueues = new HashMap<>();
    private BlockingQueue<SubjectTask> cardFixedWindowQueue;
    private BlockingQueue<SubjectTask> merchantFixedWindowQueue;
    private Map<String, BlockingQueue<SubjectTask>> customFixedWindowQueues = new HashMap<>();

    private List<SubjectWorker> workers = new ArrayList<>();

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
        initializeWorkers();
    }

    private void initializeWorkers() {
        int queueCapacity = 10000;

        // Card Sliding
        if (RulesConfig.cardSubjectPresent) {
            for (int i = 0; i < RulesConfig.rulesMapArrayForCardSubject.size(); i++) {
                BlockingQueue<SubjectTask> q = new ArrayBlockingQueue<>(queueCapacity);
                cardQueues.add(q);
                for (int w = 0; w < sessionPoolSize; w++) {
                    SubjectWorker worker = new SubjectWorker(q, Subject.CARD, w);
                    workers.add(worker);
                    executor.submit(worker);
                }
            }
        }

        // Card Fixed
        if (RulesConfig.cardSubjectFixedWindowPresent) {
            cardFixedWindowQueue = new ArrayBlockingQueue<>(queueCapacity);
            for (int w = 0; w < sessionPoolSize; w++) {
                SubjectWorker worker = new SubjectWorker(cardFixedWindowQueue, Subject.CARD, w);
                workers.add(worker);
                executor.submit(worker);
            }
        }

        // Merchant Sliding
        if (RulesConfig.merchantSubjectPresent) {
            for (int i = 0; i < RulesConfig.rulesMapArrayForMerchantSubject.size(); i++) {
                BlockingQueue<SubjectTask> q = new ArrayBlockingQueue<>(queueCapacity);
                merchantQueues.add(q);
                for (int w = 0; w < sessionPoolSize; w++) {
                    SubjectWorker worker = new SubjectWorker(q, Subject.MERCHANT, w);
                    workers.add(worker);
                    executor.submit(worker);
                }
            }
        }

        // Merchant Fixed
        if (RulesConfig.merchantSubjectFixedWindowPresent) {
            merchantFixedWindowQueue = new ArrayBlockingQueue<>(queueCapacity);
            for (int w = 0; w < sessionPoolSize; w++) {
                SubjectWorker worker = new SubjectWorker(merchantFixedWindowQueue, Subject.MERCHANT, w);
                workers.add(worker);
                executor.submit(worker);
            }
        }

        // Custom Sliding
        if (RulesConfig.customSubjectPresent) {
            for (String customSubject : RulesConfig.rulesMapForCustomSubject.keySet()) {
                BlockingQueue<SubjectTask> q = new ArrayBlockingQueue<>(queueCapacity);
                customQueues.put(customSubject, q);
                for (int w = 0; w < sessionPoolSize; w++) {
                    SubjectWorker worker = new SubjectWorker(q, Subject.CUSTOM, w);
                    workers.add(worker);
                    executor.submit(worker);
                }
            }
        }

        // Custom Fixed
        if (RulesConfig.customSubjectFixedWindowPresent) {
            for (String customSubject : RulesConfig.rulesMapForCustomSubjectFixedWindow.keySet()) {
                BlockingQueue<SubjectTask> q = new ArrayBlockingQueue<>(queueCapacity);
                customFixedWindowQueues.put(customSubject, q);
                for (int w = 0; w < sessionPoolSize; w++) {
                    SubjectWorker worker = new SubjectWorker(q, Subject.CUSTOM, w);
                    workers.add(worker);
                    executor.submit(worker);
                }
            }
        }
        
        logger.info("Initialized {} worker threads", workers.size());
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


    /**
     * Executes the Drools session for the given measurment.
     *
     * @param measurment The measurment to process.
     * @param extendedSubject    The subject of the measurment.
     * @param correlationId The correlation ID for logging.
     * @throws Exception If an error occurs during session execution.
     */
    private void executeSession(Measurment measurment, String extendedSubject, String correlationId, DroolsSession session)
            throws Exception {

        if (measurment == null) {
            throw new IllegalArgumentException("executeSession: Measurment cannot be null");
        }
        if (measurment.getAlertSet() == null) {
            measurment.setAlertSet(new AlertSet());
        }
        try {
            session.execute(measurment, extendedSubject, correlationId);
        } catch (Exception e) {
            logger.error("Error executing session for subject: {}", extendedSubject, e);
            throw e;
        }
    }

    @SuppressWarnings("unused")
    private void warmUpDrools(String subject, DroolsSession s) {
        VirtualRecordTransaction dummyTrx = TransactionDummyHelper.dummyTransaction();
        Measurment m = createNewMeasument("card-1", subject, null, 10000L);
        m.setTransaction(new VRTransactionSummary(dummyTrx));
        try {
            // on warm-up la session fournie
            executeSession(m, subject, "XXXXX", s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Measurment createNewMeasument(String key, String subject, String customSubject, Long windowSize) {
        Measurment measurment = new Measurment();
        measurment.setKey(key);
        measurment.setSubject(subject);
        measurment.setCustomSubject(customSubject);
        measurment.setWindowSize(windowSize);
        measurment.setTrxEntries(new ArrayList<>());
        return measurment;
    }

    private AlertSet newAlertSet(TrxOrAlertEvent event, String subject, String customSubject) {
        AlertSet alertSet = new AlertSet();
        switch (subject) {
            case Subject.CARD:
                alertSet.setKey(event.getTransaction().getCardId());
                break;
            case Subject.MERCHANT:
                alertSet.setKey(event.getTransaction().getMerchant());
                break;
            case Subject.CUSTOM:
                VRTransactionSummary trx = event.getTransaction();
                String key;
                try {
                    key = customSubject == null || customSubject.isEmpty() ? trx.getKey("cardId") : trx.getKey(customSubject);
                } catch (IllegalArgumentException | NoSuchFieldException e) {
                    e.printStackTrace();
                    logger.error("Error getting key from transaction with Transaction No {} and customSubject '{}', setting an empty key", trx.getTransactionNo(), customSubject);
                    key = "";
                }
                alertSet.setKey(key);
                break;
            default:
                break;
        }
        alertSet.setSubject(subject);
        alertSet.setCustomSubject(customSubject);
        alertSet.setTopic(event.getTransaction().getTopic());
        alertSet.setAlertStatus(AlertSet.NO_ALERT);
        alertSet.setTransactionNo(event.getTransaction().getTransactionNo());
        alertSet.setAlerts(new HashSet<>());
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
    private Measurment processSlidingWindow(Measurment measurment, TrxOrAlertEvent event, Long arrivalTime, String correlationId, DroolsSession session) {

        if (measurment == null) {
            throw new RuntimeException("processWindowSize: measurment cannot be null");
        }

        VRTransactionSummary transaction = event.getTransaction();
        if (transaction == null) {
            throw new RuntimeException("processWindowSize: transaction cannot be null");
        }

        transaction.setTimestamp(arrivalTime);

        List<TrxEntry> allTrx = measurment.getTrxEntries();
        if (allTrx == null) {
            throw new RuntimeException("processWindowSize: allTrx cannot be null");
        }

        List<TrxEntry> expiredTrx = new ArrayList<>();

        for (Iterator<TrxEntry> it = allTrx.iterator(); it.hasNext();) {
            TrxEntry trxEntry = it.next();
            if (measurment.expired(trxEntry)) {
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
            logger.debug("Time {} ms [{}] [{}] win={} key={} ProcessFunction: Substract delta for expired trx {}", (endSubstract - beginSubstract), correlationId, measurment.getSubject(), TimeConversion.toHumanReadableDuration(measurment.getWindowSize()), measurment.getKey(), trxEntry.getTxNo());
        }

        Long cloneStart = System.currentTimeMillis();
        Measurment initialMeasurment = measurment.clone();
        Long cloneEnd = System.currentTimeMillis();
        logger.debug("Time {} ms [{}] [{}] win={} key={} trx={} ProcessFunction: Cloning measurment", (cloneEnd - cloneStart), correlationId, measurment.getSubject(), TimeConversion.toHumanReadableDuration(measurment.getWindowSize()), measurment.getKey(), transaction.getTransactionNo());

        // Set the current transaction in the measurment for drools processing
        measurment.setTransaction(transaction);

        try {
            // update the indicators in the measurment (with drools)
            Long beginExecute = System.currentTimeMillis();

            String extendedSubject = measurment.getSubject();
            if (extendedSubject.equalsIgnoreCase(Subject.CUSTOM)) {
                extendedSubject = measurment.getSubject() + KEY_SEPARATOR + measurment.getCustomSubject();
            }

            executeSession(measurment, extendedSubject, correlationId, session);
            Long endExecute = System.currentTimeMillis();
            String subjectKey=measurment.getSubject();
            if (Subject.CUSTOM.equals(measurment.getSubject())) {
                subjectKey=measurment.getSubject()+KEY_SEPARATOR+measurment.getCustomSubject();
            }

            logger.debug("Time {} ms [{}] [{}] win={} key={} ProcessFunction: executeSession() duration", (endExecute - beginExecute), correlationId, subjectKey, TimeConversion.toHumanReadableDuration(measurment.getWindowSize()), measurment.getKey());

        } catch (Exception e) {
            logger.error("Error inserting and executing transaction in session: {}", e.getMessage());
            e.printStackTrace();
        }

        if (measurment.getAlertSet() != null && measurment.getAlertSet().hasAlerts()) {
            logger.debug("[{}] [{}] win={} key={} trx={} ProcessFunction: Found {} alerts", correlationId, measurment.getSubject(), TimeConversion.toHumanReadableDuration(measurment.getWindowSize()), measurment.getKey(), transaction.getTransactionNo(), measurment.getAlertSet().getAlerts().size());
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

        String subjectKey = measurment.getSubject();
        if (Subject.CUSTOM.equals(measurment.getSubject())) {
            subjectKey=measurment.getSubject()+KEY_SEPARATOR+measurment.getCustomSubject();
        }
        logger.debug("Time {} ms [{}] [{}] win={} key={} trx={} ProcessFunction: Delta creation for trx {}", (endCreateDelta - beginCreateDelta), correlationId, subjectKey, TimeConversion.toHumanReadableDuration(measurment.getWindowSize()), measurment.getKey(), trxEntry.getTxNo());

        // add the transaction with its delta to the measurment
        measurment.getTrxEntries().add(trxEntry);

        // update the alert set
        measurment.setAlertSet(completeAlertSet(measurment, event));
        return measurment;
    }

    /**
     * Processes a fixed window by updating the measurement windows and merging
     * alert sets.
     *
     * @param wm            The wrapper measurement object.
     * @param event         The transaction or alert event.
     * @param correlationId The correlation ID for logging.
     * @return The processed fixed window.
     */
    private WrapperMeasurment processFixedWindow(WrapperMeasurment wm, TrxOrAlertEvent event, String correlationId, DroolsSession session) {

        long now = System.currentTimeMillis();

        VRTransactionSummary trx = event.getTransaction();
        if (trx == null) {
            throw new IllegalArgumentException("Transaction in event is null");
        }

        if (trx.getTimestamp() == null) {
            trx.setTimestamp(now);
        }

        if (trx.getTimestamp() > wm.getWindowEndTime()) { // the fixed window expired
            Measurment m = createNewMeasument(wm.getMeasurment().getKey(), wm.getMeasurment().getSubject(), wm.getMeasurment().getCustomSubject(), wm.getMeasurment().getWindowSize());
            wm = WrapperMeasurment.createNewWrapperMeasurment(m, trx.getTimestamp());
        }

        // Set the current transaction in the measurment for drools processing
        wm.getMeasurment().setTransaction(trx);

        long t0 = System.nanoTime();
        Long tend = t0;
        try {
            String extendedSubject = wm.getMeasurment().getSubject();
            if (extendedSubject.equalsIgnoreCase(Subject.CUSTOM)) {
                extendedSubject = wm.getMeasurment().getSubject() + KEY_SEPARATOR + wm.getMeasurment().getCustomSubject();
            }

            // update the indicators in the measurment (with drools)
            executeSession(wm.getMeasurment(), extendedSubject, correlationId, session);
        } catch (Exception e) {
            logger.error("Error inserting and executing transaction in session: {}", e.getMessage());
            e.printStackTrace();
        }
        finally {
            tend = System.nanoTime();
            logger.debug("Time {} ms [{}] win={} key={} ProcessFunction: Drools execution time for trx {}", (tend - t0) / 1_000_000L, correlationId, TimeConversion.toHumanReadableDuration(wm.getMeasurment().getWindowSize()), wm.getMeasurment().getKey(), wm.getMeasurment().getTransaction().getTransactionNo());
        }

        // Update the AlertSet
        wm.getMeasurment().setAlertSet(completeAlertSet(wm.getMeasurment(), event));
        return wm;      
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
    private AlertSet completeAlertSet(Measurment measurment, TrxOrAlertEvent event) {

        if (measurment.getAlertSet() == null) {
            logger.warn("AlertSet is null, should not happen");
            return null;
        }
        measurment.getAlertSet().setTransactionNo(event.getTransaction().getTransactionNo());

        AlertSet alertSet = measurment.getAlertSet();
        alertSet.setTopic(event.getTransaction().getTopic());

        if (alertSet.hasAlerts()) {
            alertSet.setAlertStatus(AlertSet.ALERT);
        } else {
            alertSet.setAlertStatus(AlertSet.NO_ALERT);
        }

        switch (measurment.getSubject()) {
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
        if (originalAlertSet == null || !originalAlertSet.hasAlerts()) {
            if (newAlertSet != null) {
                newAlertSet.calculateScore(RulesConfig.alertRulesCount);
            }
            return newAlertSet != null ? newAlertSet : new AlertSet();
        }
        if (newAlertSet == null || !newAlertSet.hasAlerts()) {
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

        if (!originalAlertSet.hasAlerts()) {
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
        String correlationId = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-correlation-id");

        try {
            // Désérialiser
            long beforeDeserialize = System.currentTimeMillis();
            @SuppressWarnings("unchecked")
            FraudCheckRequest<ITransaction> request = (FraudCheckRequest<ITransaction>) SerializationManager.deserialize(msg.getData());
            long afterDeserialize = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} process(): Deserialization Time", (afterDeserialize - beforeDeserialize), correlationId, Thread.currentThread().getName(), request.getContent().getTransactionNo());

            VRTransactionSummary tx = new VRTransactionSummary(request.getContent());
            tx.setTopic(topic);
            tx.setDateTimeOfTrx(tx.getAutTranDateTimeF007());

            long recv0 = System.currentTimeMillis();
            String sClientTs = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-client-publish-ts-ms");
            String sRecvTs = msg.getHeaders() == null ? null : msg.getHeaders().getFirst("x-recv-ts-ms");

            long clientTs = sClientTs != null ? Long.parseLong(sClientTs) : 0L;
            long recvTs = sRecvTs != null ? Long.parseLong(sRecvTs) : recv0;
            long publishToReceiveMs = recvTs - clientTs;
            logger.debug("Time {} ms [{}] [Thread {}] trx={} Nats: Time between API publish and fraudmanager reception", publishToReceiveMs, correlationId, Thread.currentThread().getName(), tx.getTransactionNo());

            String cardKey = CARD_KEY_PREFIX + tx.getCardId();
            String merchantKey = MERCHANT_KEY_PREFIX + tx.getMerchant();

            // create the initial event
            TrxOrAlertEvent event = new TrxOrAlertEvent(tx, null, correlationId);

            // parallel processing of subjects with CompletableFutures
            List<CompletableFuture<TrxOrAlertEvent>> cardProcessingFutures = new ArrayList<>();
            List<CompletableFuture<TrxOrAlertEvent>> merchantProcessingFutures = new ArrayList<>();
            List<CompletableFuture<TrxOrAlertEvent>> customProcessingFutures = new ArrayList<>();

            Long endArrivalTime = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} process(): Pre-processing Time before starting the threads", (endArrivalTime - arrivalTime), correlationId, Thread.currentThread().getName(), tx.getTransactionNo());

            if (RulesConfig.cardSubjectPresent) { 
                for (int i = 0; i < RulesConfig.rulesMapArrayForCardSubject.size(); i++) {
                    Map<Long, List<RuleDefinition>> ruleMapForCard = RulesConfig.rulesMapArrayForCardSubject.get(i);
                    CompletableFuture<TrxOrAlertEvent> future = new CompletableFuture<>();
                    SubjectTask task = new SubjectTask(event, cardKey, arrivalTime, correlationId, Subject.CARD, null, ruleMapForCard, false, future);
                    try {
                        cardQueues.get(i).put(task);
                        cardProcessingFutures.add(future);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        future.completeExceptionally(e);
                    }
                }
            }

            if (RulesConfig.cardSubjectFixedWindowPresent) { 
                CompletableFuture<TrxOrAlertEvent> future = new CompletableFuture<>();
                SubjectTask task = new SubjectTask(event, cardKey, arrivalTime, correlationId, Subject.CARD, null, RulesConfig.rulesMapForCardSubjectFixedWindow, true, future);
                try {
                    cardFixedWindowQueue.put(task);
                    cardProcessingFutures.add(future);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    future.completeExceptionally(e);
                }
            }

            if (RulesConfig.merchantSubjectPresent) { 
                for (int i = 0; i < RulesConfig.rulesMapArrayForMerchantSubject.size(); i++) {
                    Map<Long, List<RuleDefinition>> ruleMapForMerchant = RulesConfig.rulesMapArrayForMerchantSubject.get(i);
                    CompletableFuture<TrxOrAlertEvent> future = new CompletableFuture<>();
                    SubjectTask task = new SubjectTask(event, merchantKey, arrivalTime, correlationId, Subject.MERCHANT, null, ruleMapForMerchant, false, future);
                    try {
                        merchantQueues.get(i).put(task);
                        merchantProcessingFutures.add(future);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        future.completeExceptionally(e);
                    }
                }
            }

            if (RulesConfig.merchantSubjectFixedWindowPresent) { 
                CompletableFuture<TrxOrAlertEvent> future = new CompletableFuture<>();
                SubjectTask task = new SubjectTask(event, merchantKey, arrivalTime, correlationId, Subject.MERCHANT, null, RulesConfig.rulesMapForMerchantSubjectFixedWindow, true, future);
                try {
                    merchantFixedWindowQueue.put(task);
                    merchantProcessingFutures.add(future);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    future.completeExceptionally(e);
                }
            }

            if (RulesConfig.customSubjectPresent) { 
                for (String customSubject : RulesConfig.rulesMapForCustomSubject.keySet()) {
                    String keySpec = customSubject;
                    String keyValue = event.getTransaction().getKey(keySpec);
                    String customSubjectKey = CUSTOM_KEY_PREFIX + keyValue;
                    
                    CompletableFuture<TrxOrAlertEvent> future = new CompletableFuture<>();
                    SubjectTask task = new SubjectTask(event, customSubjectKey, arrivalTime, correlationId, Subject.CUSTOM, customSubject, RulesConfig.rulesMapForCustomSubject.get(customSubject), false, future);
                    
                    try {
                        customQueues.get(customSubject).put(task);
                        customProcessingFutures.add(future);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        future.completeExceptionally(e);
                    }
                }
            }

            if (RulesConfig.customSubjectFixedWindowPresent) { 
                for (String customSubject : RulesConfig.rulesMapForCustomSubjectFixedWindow.keySet()) {
                    String keySpec = customSubject;
                    String keyValue = event.getTransaction().getKey(keySpec);
                    String customSubjectKey = CUSTOM_KEY_PREFIX + keyValue;

                    CompletableFuture<TrxOrAlertEvent> future = new CompletableFuture<>();
                    SubjectTask task = new SubjectTask(event, customSubjectKey, arrivalTime, correlationId, Subject.CUSTOM, customSubject, RulesConfig.rulesMapForCustomSubjectFixedWindow.get(customSubject), true, future);
                    
                    try {
                        customFixedWindowQueues.get(customSubject).put(task);
                        customProcessingFutures.add(future);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        future.completeExceptionally(e);
                    }
                }
            }

            // Attendre que tous les traitements se terminent et récupérer les résultats
            List<CompletableFuture<TrxOrAlertEvent>> futures = new java.util.ArrayList<>();
            if (cardProcessingFutures != null && !cardProcessingFutures.isEmpty()) {
                futures.addAll(cardProcessingFutures);
            }
            if (merchantProcessingFutures != null && !merchantProcessingFutures.isEmpty()) {
                futures.addAll(merchantProcessingFutures);
            }
            if (customProcessingFutures != null && !customProcessingFutures.isEmpty()) {
                futures.addAll(customProcessingFutures);
            }

            CompletableFuture<Void> allProcessing = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allProcessing.join();
            Long endOfJoin = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} process(): Duration of threads completion", endOfJoin - endArrivalTime, correlationId, Thread.currentThread().getName(), tx.getTransactionNo());

            // Récupérer les résultats
            AlertSet combinedAlertSet = new AlertSet();

            try {
                for (CompletableFuture<TrxOrAlertEvent> future : futures) {
                    TrxOrAlertEvent e = future != null ? future.get() : null;
                    if (e != null) {
                        combinedAlertSet = mergeAlertSets(combinedAlertSet, e.getAlertSet());
                    }
                }
            } catch (Exception e) {
                logger.warn("Error retrieving processing results", e);
            }

            Long resultAgregationEnd = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} duration of results aggregation", (resultAgregationEnd - endOfJoin), correlationId, Thread.currentThread().getName(), tx.getTransactionNo());

            FraudCheckResponse response = new FraudCheckResponse();
            response.setAlertSet(combinedAlertSet);
            response.setCorrelationId(correlationId);
            response.setTimestamp(System.currentTimeMillis());
            response.setError(false);
            response.setErrorMessage(NO_ERROR_MESSAGE);

            // Sérialiser et publier réponse
            Long beforeSerialize = System.currentTimeMillis();
            byte[] responseBytes = SerializationManager.serialize(response);
            Long afterSerialize = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [Thread {}] trx={} process(): Serialization Time", (afterSerialize - beforeSerialize), correlationId, Thread.currentThread().getName(), tx.getTransactionNo());
            natsConnection.publish(topic, responseBytes);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error processing transaction", e);
            FraudCheckResponse response = new FraudCheckResponse();
            response.setAlertSet(null);
            response.setCorrelationId(correlationId);
            response.setTimestamp(System.currentTimeMillis());
            response.setError(true);
            response.setErrorMessage(e.getMessage());
            byte[] responseBytes=null;
            try {
                responseBytes = SerializationManager.serialize(response);
            } catch (IOException e1) {
                e1.printStackTrace();
                logger.error("Error serializing response", e1);
            }
            natsConnection.publish(topic, responseBytes);
        }
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
    private TrxOrAlertEvent processEvent(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId, String subject, String customSubject, Map<Long, List<RuleDefinition>> ruleMap, Boolean fixedWindow, DroolsSession session) {

        AlertSet alertSet = newAlertSet(event, subject, customSubject);
        TrxOrAlertEvent processedEvent = buildEvent(event, alertSet);
        String suffix = customSubject == null || customSubject.isEmpty() ? "" : customSubject + KEY_SEPARATOR;
        String eventKey = subject + KEY_SEPARATOR + suffix + key;

        Map<Long, List<RuleDefinition>> processedRulesMap = subject.equals(Subject.CUSTOM) ? RulesConfig.rulesMapForCustomSubject.get(customSubject) : ruleMap;

        if (!fixedWindow) {

            for (Entry<Long, List<RuleDefinition>> entry : processedRulesMap.entrySet()) {

                Long ruleWindowSize = (Long) entry.getKey();
                Long stateGetStart = System.currentTimeMillis();
                Measurment measurment = rocksDBService.getMeasurmentByKey(eventKey + WINDOW_SEPARATOR + ruleWindowSize);
                Long stateGetEnd = System.currentTimeMillis();
                logger.debug("Time {} ms [{}] [{}] [Thread : {}] win={} key={} trx={} processEvent: State get time", (stateGetEnd - stateGetStart), correlationId, subject + (customSubject != null ? ":" + customSubject : ""), Thread.currentThread().getName(), TimeConversion.toHumanReadableDuration(ruleWindowSize), key, processedEvent.getTransaction().getTransactionNo());

                if (measurment == null) {
                    measurment = createNewMeasument(key, subject, customSubject, ruleWindowSize);
                }

                Long beforeProcessWindowSize = System.currentTimeMillis();
                measurment = processSlidingWindow(measurment, processedEvent, arrivalTime, correlationId, session);
                Long afterProcessWindowSize = System.currentTimeMillis();
                logger.debug("Time {} ms [{}] [{}] [Thread : {}] key={} trx={} processEvent: Duration of processSlidingWindow({})", (afterProcessWindowSize - beforeProcessWindowSize), correlationId, subject + (customSubject != null ? ":" + customSubject : ""), Thread.currentThread().getName(), key, processedEvent.getTransaction().getTransactionNo(), TimeConversion.toHumanReadableDuration(ruleWindowSize));

                if (measurment.getAlertSet() != null &&  measurment.getAlertSet().hasAlerts()) {
                    logger.debug("****************Alerts found: {} in one future*********************", measurment.getAlertSet().getAlerts().size());
                }

                alertSet = mergeAlertSets(alertSet, measurment.getAlertSet());
                measurment.setAlertSet(null); // clear alert set to avoid duplication in next window
                measurment.setTransaction(null); // clear transaction to avoid serialization issues
                measurment.setGlobalRecords(null); // clear global records because they don't need to be stored with the timeramed measurment

                // update the state of the measurment in the database
                rocksDBService.setMeasurmentByKey(eventKey + WINDOW_SEPARATOR + measurment.getWindowSize(), measurment);
            }
            
        } else { // fixed window
            for (Entry<Long, List<RuleDefinition>> entry : ruleMap.entrySet()) {
                Long ruleWindowSize = (Long) entry.getKey();
                String fwEventKey = FIXED_WINDOW_PREFIX + eventKey;
                Long beginProcessingWindow = System.currentTimeMillis();

                // récupérer la mesure stockée pour cette clé depuis le ValueState
                WrapperMeasurment wm=null;
                try {
                    // retrieve the state of the fixed window from the database
                    long retrieveBegin = System.currentTimeMillis();
                    wm = rocksDBService.getWrapperMeasurmentByKey(fwEventKey + WINDOW_SEPARATOR + ruleWindowSize);
                    long retrieveEnd = System.currentTimeMillis();
                    logger.debug("Time {} ms [{}] [{}] win={} key={} ProcessFunction: Retrieving state for window {}", (retrieveEnd - retrieveBegin), correlationId, subject + (customSubject != null ? ":" + customSubject : ""), TimeConversion.toHumanReadableDuration(ruleWindowSize), fwEventKey, TimeConversion.toHumanReadableDuration(ruleWindowSize));
                }
                catch (Exception e) {
                    logger.error("FixedWindows: Error retrieving measurement state: {}", e.getMessage());
                    e.printStackTrace();
                }

                if (wm == null) {
                    wm = WrapperMeasurment.createNewWrapperMeasurment(createNewMeasument(key, subject, customSubject, ruleWindowSize), processedEvent.getTransaction().getTimestamp());
                }
            
                wm = processFixedWindow(wm, processedEvent, correlationId, session);

                alertSet = mergeAlertSets(alertSet, wm.getMeasurment().getAlertSet());
                wm.getMeasurment().setAlertSet(null); // clear alert set to avoid duplication in next window
                wm.getMeasurment().setTransaction(null); // clear transaction to avoid serialization issues
                wm.getMeasurment().setGlobalRecords(null); // clear global records to avoid serialization issues
                // update the state
                try {
                    long updateBegin = System.currentTimeMillis();
                    rocksDBService.setWrapperMeasurmentByKey(fwEventKey + WINDOW_SEPARATOR + ruleWindowSize, wm);
                    long updateEnd = System.currentTimeMillis();
                    logger.debug("Time {} ms [{}] FixedWindows: [{}] win={} key={} ProcessFunction: Updating state for window {}", (updateEnd - updateBegin), correlationId, subject + (customSubject != null ? ":" + customSubject : ""), TimeConversion.toHumanReadableDuration(ruleWindowSize), fwEventKey, TimeConversion.toHumanReadableDuration(ruleWindowSize));
                } catch (Exception e) {
                    logger.error("FixedWindows:Error updating measurement state: {}", e.getMessage());
                    e.printStackTrace();
                }
                long endProcessingWindow = System.currentTimeMillis();
                logger.debug("Time {} ms FixedWindows: [{}] [{}] [Trx {}] ProcessFunction: Time of processing fixed window {}", (endProcessingWindow - beginProcessingWindow), correlationId, subject + (customSubject != null ? ":" + customSubject : ""), event.getTransaction().getTransactionNo(), TimeConversion.toHumanReadableDuration(ruleWindowSize));
            }
        }

        processedEvent.setAlertSet(alertSet);
        return processedEvent;
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

        workers.forEach(SubjectWorker::stop);
        workers.clear();
        cardQueues.clear();
        merchantQueues.clear();
        customQueues.clear();
        if(cardFixedWindowQueue != null) cardFixedWindowQueue.clear();
        if(merchantFixedWindowQueue != null) merchantFixedWindowQueue.clear();
        customFixedWindowQueues.clear();
        logger.info("FraudProcessor shut down.");

    }

    private static class SubjectTask {
        TrxOrAlertEvent event;
        String key;
        Long arrivalTime;
        String correlationId;
        String subject;
        String customSubject;
        Map<Long, List<RuleDefinition>> ruleMap;
        Boolean fixedWindow;
        CompletableFuture<TrxOrAlertEvent> future;

        public SubjectTask(TrxOrAlertEvent event, String key, Long arrivalTime, String correlationId, String subject, String customSubject, Map<Long, List<RuleDefinition>> ruleMap, Boolean fixedWindow, CompletableFuture<TrxOrAlertEvent> future) {
            this.event = event;
            this.key = key;
            this.arrivalTime = arrivalTime;
            this.correlationId = correlationId;
            this.subject = subject;
            this.customSubject = customSubject;
            this.ruleMap = ruleMap;
            this.fixedWindow = fixedWindow;
            this.future = future;
        }
    }

    private class SubjectWorker implements Runnable {
        private final BlockingQueue<SubjectTask> queue;
        private final DroolsSession session;
        private final String workerName;
        private volatile boolean running = true;

        public SubjectWorker(BlockingQueue<SubjectTask> queue, String subject, int id) {
            this.queue = queue;
            this.session = createSessionForSubject(subject);
            this.workerName = "Worker-" + subject + "-" + id;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    SubjectTask task = queue.take();
                    try {
                        TrxOrAlertEvent result = keyProcessor.executeWithLock(task.key, (k) -> {
                            return processEvent(task.event, k, task.arrivalTime, task.correlationId, task.subject, task.customSubject, task.ruleMap, task.fixedWindow, session);
                        });
                        task.future.complete(result);
                    } catch (Exception e) {
                        logger.error("Error processing task in " + workerName, e);
                        task.future.completeExceptionally(e);
                    } finally {
                        try {
                            session.clean();
                        } catch (Exception e) {
                            logger.error("Error cleaning session in " + workerName, e);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
            // Cleanup
            if (session != null) {
                try {
                    session.dispose();
                } catch (Exception e) {
                    logger.error("Error disposing session in " + workerName, e);
                }
            }
        }

        public void stop() {
            running = false;
        }
    }
}

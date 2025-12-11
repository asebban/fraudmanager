package ma.s2m.fraudmanager.service.processors;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Connection;
import io.nats.client.Message;
import ma.medtech.droolbuilder.utils.TimeConversion;
import ma.s2m.auth.query.FraudQueryRequest;
import ma.s2m.auth.query.FraudQueryResponse;
import ma.s2m.auth.query.Indicator;
import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.model.MeasurmentRecord;
import ma.s2m.fraudmanager.model.RecordHashMap;
import ma.s2m.fraudmanager.service.db.IStoreService;
import ma.s2m.serializer.SerializationManager;

public class QueryProcessor {

    @SuppressWarnings("unused")
    private Connection nc;
    @SuppressWarnings("unused")
    private IStoreService storageService;
    private Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

    public QueryProcessor(IStoreService storageService, Connection nc) {
        this.storageService = storageService;
        this.nc = nc;
    }

    public void process(Message msg) {
        byte[] data = msg.getData();

        FraudQueryRequest request = null;

        try {
            request = (FraudQueryRequest) SerializationManager.deserialize(data);
        } catch (ClassNotFoundException | IOException e) {
            logger.error("Error while deserializing message. Query service is unavailable", e);
            e.printStackTrace();
            throw new RuntimeException("Error while deserializing message. Query service is unavailable", e);
        }

        String subject = request.getSubject();
        request.setTopic(msg.getReplyTo());
        
        if (subject == null) {
            logger.error("Query service: Request subject is null");
            throw new RuntimeException("Query service: Request subject is null");
        }

        FraudQueryResponse response = new FraudQueryResponse();
        response.setKey(request.getKey());
        response.setTimeframe(request.getTimeframe());
        response.setSubject(request.getSubject());
        response.setTopic(request.getTopic());
        response.setCorrelationId(request.getCorrelationId());

        String subjectKey = request.getSubject() + ":" + request.getKey();

        List<String> keys = this.storageService.getKeysStartingWith(subjectKey);

        Long windowSize = 0L;
        for (String key : keys) {
            try {
                windowSize = Long.parseLong(key.split(FraudProcessor.WINDOW_SEPARATOR)[1]);
            } catch(Exception e) {
                // Here it will be the global record key
                continue;
            }

            if (request.getTimeframe() != null && request.getTimeframe() != 0L) {
                if (!request.getTimeframe().equals(windowSize)) {
                    continue;
                }
            }

            String specificKey = subjectKey + FraudProcessor.WINDOW_SEPARATOR + windowSize;

            Measurment measurment = this.storageService.getMeasurmentByKey(specificKey);
            if (measurment != null) {
                for (Map.Entry<String, MeasurmentRecord> e : measurment.getRecords().getRecordHashMap().entrySet()) {
                    Indicator indicator = new Indicator();
                    MeasurmentRecord measurmentRecord = e.getValue();
                    indicator.setCount(measurmentRecord.getCount());
                    indicator.setAmount(measurmentRecord.getAmount());
                    indicator.setValues(measurmentRecord.getValues());
                    indicator.setArgList(measurmentRecord.getArgList());
                    indicator.setArgSet(measurmentRecord.getArgSet());
                    response.getRecords().put(e.getKey() + "-" + TimeConversion.toHumanReadableDuration(windowSize), indicator);
                }
            }
        }

        RecordHashMap globalRecords = this.storageService.getRecordHashMapByKey(subjectKey + FraudProcessor.WINDOW_SEPARATOR + FraudProcessor.GLOBAL_RECORD_KEY_SUFFIX);
        if (globalRecords != null && globalRecords.getRecordHashMap() != null) {
            for (Map.Entry<String, MeasurmentRecord> e : globalRecords.getRecordHashMap().entrySet()) {
                Indicator indicator = new Indicator();
                MeasurmentRecord measurmentRecord = e.getValue();
                indicator.setCount(measurmentRecord.getCount());
                indicator.setAmount(measurmentRecord.getAmount());
                indicator.setValues(measurmentRecord.getValues());
                indicator.setArgList(measurmentRecord.getArgList());
                indicator.setArgSet(measurmentRecord.getArgSet());
                response.getRecords().put(e.getKey(), indicator);
            }
        }


        try {
            data = SerializationManager.serialize(response);
            nc.publish(request.getTopic(), data);
        } catch (IOException e) {
            logger.error("Error while serializing / publishing message to Nats", e);
            e.printStackTrace();
            throw new RuntimeException("Error while serializing / publishing message to Nats", e);

        }
    }
}

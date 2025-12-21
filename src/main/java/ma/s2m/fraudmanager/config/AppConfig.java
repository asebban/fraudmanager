package ma.s2m.fraudmanager.config;

import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.s2m.fraudmanager.service.NatsService;
import ma.s2m.fraudmanager.service.db.EclipseStoreService;
import ma.s2m.fraudmanager.service.db.IStoreService;
import ma.s2m.fraudmanager.service.db.RocksDBService;
import ma.s2m.fraudmanager.service.db.StorageConfig;
import ma.s2m.fraudmanager.service.processors.FraudProcessor;
import ma.s2m.fraudmanager.service.processors.QueryProcessor;
import ma.s2m.fraudmanager.util.PropertiesLoader;
import ma.s2m.functions.Function;
import ma.s2m.repository.IRepository;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private static final String NATS_PROTOCOL = "nats://";
    private static final String SHARD_SEPARATOR = ",";
    private static final String SHARD_RANGE_SEPARATOR = "-";
    public static String natsHost;
    public static int natsPort;
    public static String natsTopic;
    public static String natsAlertSetTopic;
    public static String natsRuleUpdateTopic;
    public static String redisHost;
    public static int redisPort;
    public static String redisUser;
    public static String redisPassword;
    public static String redisDatabase;
    public static int appThreadSessionPoolSize;
    public static int appProcessorThreads;
    public static String ruleDeploymentDir;
    public static Boolean droolsDebugEnabled = false;
    public static Boolean droolsProfilerEnabled = false;
    public static Boolean droolsRulesAgendaGroupRuleTypeEnabled = false;
    public static String appProcessorMessagingProvider = "";
    public static Boolean appStorageCleanOnShutdown = false;
    public static String repositoryWorkspaceDirectory = "";
    public static String storagePath = "";
    public static int storageQueueSize = 10_000;
    public static int appProcessorSubjectParallelismThreshold = 10;
    public static String appProcessorRepositoryClass = "";
    public static Long waitTime = 300000L;
    public static String natsQueryTopic = "";
    public static int storageMemoryShardCount = 4;
    public static long storageSubmitTimeoutMs = 100;
    public static long storageFlushIntervalMs = 150;
    public static int storageDiskShardCount = 64;
    public static String configFile = "";
    public static String nodeName = "";
    public static List<Integer> storageShards = new ArrayList<>();
    public static Boolean fraudManagerMultiNode = false;
    public static Boolean fraudManagerAlertStoringEnabled = false;
    public static String appStorageType = "rocksdb";
    public static boolean metricsEnabled = false;
    public static int metricsPort = 9464;
    public static String metricsPath = "/metrics";
    public static int appRulesReloadRetryIntervalSeconds = 1;
    public static int appRulesReloadMaxRetries = 3;

    public AppConfig(String[] args) throws Exception {

        PropertiesLoader loader = new PropertiesLoader("fraudmanager.properties");
        String repositoryClassName = loader.getProperty("app.processor.repository.class", "ma.s2m.repository.PropertiesRepository");
        IRepository centralRepository = (IRepository) Function.createDynamicClass(repositoryClassName, "fraudmanager.properties", args);
        centralRepository.load();

        natsHost = centralRepository.getProperty("nats.host", "localhost");
        natsPort = Integer.parseInt(centralRepository.getProperty("nats.port", "4222"));
        natsTopic = centralRepository.getProperty("nats.topic", "fraud.check");
        natsAlertSetTopic = centralRepository.getProperty("nats.alertset.topic", "fraud.alert");
        natsRuleUpdateTopic = centralRepository.getProperty("nats.rule.update.topic", "rule.update");

        redisHost = centralRepository.getProperty("redis.host", "localhost");
        redisPort = Integer.parseInt(centralRepository.getProperty("redis.port", "6379"));
        redisDatabase = centralRepository.getProperty("redis.database", "0");
        redisUser = centralRepository.getProperty("redis.user", "default");
        redisPassword = centralRepository.getProperty("redis.password", "");

        appThreadSessionPoolSize = Integer.parseInt(centralRepository.getProperty("app.thread.session.pool.size", "16"));
        appProcessorThreads = Integer.parseInt(centralRepository.getProperty("app.processor.threads", "16"));
        appProcessorMessagingProvider = centralRepository.getProperty("app.processor.messaging.provider", "nats");
        appProcessorRepositoryClass = centralRepository.getProperty("app.processor.repository.class", "ma.s2m.repository.PropertiesRepository");

        ruleDeploymentDir = centralRepository.getProperty("drools.rules.deployment.dir", "./rules");
        droolsDebugEnabled = Boolean.parseBoolean(centralRepository.getProperty("drools.debug.enabled", "false"));
        droolsProfilerEnabled = Boolean.parseBoolean(centralRepository.getProperty("drools.profiler.enabled", "false"));
        droolsRulesAgendaGroupRuleTypeEnabled = Boolean.parseBoolean(centralRepository.getProperty("drools.rules.agenda.group.rule.type.enabled", "false"));
        appStorageCleanOnShutdown = Boolean.parseBoolean(centralRepository.getProperty("app.storage.clean.on.shutdown", "false"));
        repositoryWorkspaceDirectory = centralRepository.getProperty("drool.builder.repository.workspace.directory", "./workspace");
        storagePath = centralRepository.getProperty("storage.path", "./storage");
        storageQueueSize = Integer.parseInt(centralRepository.getProperty("storage.queue.size", "10000"));
        appProcessorSubjectParallelismThreshold = Integer.parseInt(centralRepository.getProperty("app.processor.subject.parallelism.threshold", "10"));
        waitTime = Long.parseLong(centralRepository.getProperty("wait.time", "300000"));
        natsQueryTopic = centralRepository.getProperty("nats.query.topic", "fraud.query");
        storageMemoryShardCount = Integer.parseInt(centralRepository.getProperty("storage.memory.shard.count", "4"));
        storageSubmitTimeoutMs = Long.parseLong(centralRepository.getProperty("storage.submit.timeout.ms", "100"));
        storageDiskShardCount = Integer.parseInt(centralRepository.getProperty("storage.disk.shard.count", "64"));
        storageFlushIntervalMs = Long.parseLong(centralRepository.getProperty("storage.flush.interval.ms", "150"));
        nodeName = centralRepository.getProperty("node.name", "node-0");
        fraudManagerMultiNode = Boolean.parseBoolean(centralRepository.getProperty("fraudmanager.multinode", "false"));
        fraudManagerAlertStoringEnabled = Boolean.parseBoolean(centralRepository.getProperty("fraudmanager.alertstoring.enabled", "false"));
        appStorageType = centralRepository.getProperty("storage.type", "rocksdb");
        metricsEnabled = Boolean.parseBoolean(centralRepository.getProperty("metrics.enabled", "false"));
        metricsPort = Integer.parseInt(centralRepository.getProperty("metrics.port", "9464"));
        metricsPath = centralRepository.getProperty("metrics.path", "/metrics");
        appRulesReloadRetryIntervalSeconds = Integer.parseInt(centralRepository.getProperty("app.rules.reload.retry.interval.seconds", "1"));
        appRulesReloadMaxRetries = Integer.parseInt(centralRepository.getProperty("app.rules.reload.max.retries", "3"));

        String shardsProp = centralRepository.getProperty("storage." + nodeName + ".shards", "0");
        if (shardsProp != null && !shardsProp.isEmpty()) {
            String[] parts = shardsProp.split(SHARD_SEPARATOR);
            for (String part : parts) {
                if (part.contains(SHARD_RANGE_SEPARATOR)) {
                    String[] range = part.split(SHARD_RANGE_SEPARATOR);
                    int start = Integer.parseInt(range[0].trim());
                    int end = Integer.parseInt(range[1].trim());
                    for (int i = start; i <= end; i++) {
                        storageShards.add(i);
                    }
                } else {
                    storageShards.add(Integer.parseInt(part.trim()));
                }
            }
            logger.info("Loaded shards for node {}: {}", nodeName, storageShards);
        } else {
            logger.warn("No shards configuration found for node: {}", nodeName);
        }
    }

    public NatsService natsService() {
        try {
            Connection nc = Nats.connect(NATS_PROTOCOL + natsHost + ":" + natsPort);
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            IStoreService storeService = storeService(storagePath, storageQueueSize);
            FraudProcessor fraudProcessor = new FraudProcessor(storeService, nc);
            QueryProcessor queryProcessor = new QueryProcessor(storeService, nc);
            return new NatsService(nc, executor, fraudProcessor, queryProcessor, storeService);
        } catch (Exception e) {
            throw new RuntimeException("Error creating NatsService", e);
        }
    }

    public IStoreService storeService(String storagePath, int storageQueueSize) {
        StorageConfig cfg = new StorageConfig(
                storageDiskShardCount,
                new ArrayList<>(storageShards),
                nodeName,
                storageFlushIntervalMs,
                storageSubmitTimeoutMs,
                storageMemoryShardCount,
                ma.s2m.fraudmanager.metrics.Metrics.getRegistry()
        );
        if ("eclipsestore".equalsIgnoreCase(appStorageType)) {
            return new EclipseStoreService(storagePath, cfg);
        }
        return new RocksDBService(storagePath, storageQueueSize, cfg);
    }

    private static String ruleTypeConverter(int ruleType) {
        switch (ruleType) {
            case RuleDefinition.RULE_TYPE_ALERT:
                return "ALERT";
            case RuleDefinition.RULE_TYPE_COMPUTE:
                return "COMPUTE";
            default:
                throw new IllegalArgumentException("Unsupported rule type: " + ruleType);
        }
    }

    public static String ruleTypePrefix(int ruleType) {
        return droolsRulesAgendaGroupRuleTypeEnabled ? ruleTypeConverter(ruleType) + ":" : "";
    }

}

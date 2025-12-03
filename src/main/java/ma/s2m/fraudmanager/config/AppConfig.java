package ma.s2m.fraudmanager.config;

import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.s2m.fraudmanager.service.FraudProcessor;
import ma.s2m.fraudmanager.service.NatsService;
import ma.s2m.fraudmanager.service.QueryProcessor;
import ma.s2m.fraudmanager.service.RocksDBService;
import ma.s2m.functions.Function;
import ma.s2m.repository.IRepository;
import ma.s2m.repository.PropertiesRepository;
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
    public static String natsHost;
    public static int natsPort;
    public static String natsTopic;
    public static String natsAlertSetTopic;
    public static String redisHost;
    public static int redisPort;
    public static String redisUser;
    public static String redisPassword;
    public static String redisDatabase;
    public static int appThreadSessionPoolSize;
    public static String ruleDeploymentDir;
    public static Boolean droolsDebugEnabled = false;
    public static Boolean droolsProfilerEnabled = false;
    public static Boolean droolsRulesAgendaGroupRuleTypeEnabled = false;
    public static String appProcessorMessagingProvider = "";
    public static Boolean appRocksDBCleanOnShutdown = false;
    public static String repositoryWorkspaceDirectory = "";
    public static String rocksDBPath = "";
    public static int rocksDBQueueSize = 10_000;
    public static int appProcessorSubjectParallelismThreshold = 10;
    public static String appProcessorRepositoryClass = "";
    public static Long waitTime = 300000L;
    public static String natsQueryTopic = "";
    public static int rocksDBMemoryShardCount = 4;
    public static long rocksDBSubmitTimeoutMs = 100;
    public static int rocksDBDiskShardCount = 64;
    public static String configFile = "";
    public static String rocksdbNodeName = "";
    public static List<Integer> rocksDBShards = new ArrayList<>();
    public static Boolean fraudManagerMultiNode = false;
    public static Boolean fraudManagerAlertStoringEnabled = false;

    public AppConfig(String[] args) throws Exception {

        IRepository localPropertyFile = new PropertiesRepository("fraudmanager.properties");
        String repositoryClassName = localPropertyFile.getProperty("app.processor.repository.class", "ma.s2m.repository.PropertiesRepository");
        IRepository centralRepository = (IRepository) Function.createDynamicClass(repositoryClassName, "fraudmanager.properties", args);
        centralRepository.load();

        natsHost = centralRepository.getProperty("nats.host", "localhost");
        natsPort = Integer.parseInt(centralRepository.getProperty("nats.port", "4222"));
        natsTopic = centralRepository.getProperty("nats.topic", "fraud.check");
        natsAlertSetTopic = centralRepository.getProperty("nats.alertset.topic", "fraud.alert");

        redisHost = centralRepository.getProperty("redis.host", "localhost");
        redisPort = Integer.parseInt(centralRepository.getProperty("redis.port", "6379"));
        redisDatabase = centralRepository.getProperty("redis.database", "0");
        redisUser = centralRepository.getProperty("redis.user", "default");
        redisPassword = centralRepository.getProperty("redis.password", "");

        appThreadSessionPoolSize = Integer.parseInt(centralRepository.getProperty("app.thread.session.pool.size", "16"));
        appProcessorMessagingProvider = centralRepository.getProperty("app.processor.messaging.provider", "nats");
        appProcessorRepositoryClass = centralRepository.getProperty("app.processor.repository.class", "ma.s2m.repository.PropertiesRepository");

        ruleDeploymentDir = centralRepository.getProperty("drools.rules.deployment.dir", "./rules");
        droolsDebugEnabled = Boolean.parseBoolean(centralRepository.getProperty("drools.debug.enabled", "false"));
        droolsProfilerEnabled = Boolean.parseBoolean(centralRepository.getProperty("drools.profiler.enabled", "false"));
        droolsRulesAgendaGroupRuleTypeEnabled = Boolean.parseBoolean(centralRepository.getProperty("drools.rules.agenda.group.rule.type.enabled", "false"));
        appRocksDBCleanOnShutdown = Boolean.parseBoolean(centralRepository.getProperty("app.rocksdb.clean.on.shutdown", "false"));
        repositoryWorkspaceDirectory = centralRepository.getProperty("drool.builder.repository.workspace.directory", "./workspace");
        rocksDBPath = centralRepository.getProperty("rocksdb.path", "./rocksdb");
        rocksDBQueueSize = Integer.parseInt(centralRepository.getProperty("rocksdb.queue.size", "10000"));
        appProcessorSubjectParallelismThreshold = Integer.parseInt(centralRepository.getProperty("app.processor.subject.parallelism.threshold", "10"));
        waitTime = Long.parseLong(centralRepository.getProperty("wait.time", "300000"));
        natsQueryTopic = centralRepository.getProperty("nats.query.topic", "");
        rocksDBMemoryShardCount = Integer.parseInt(centralRepository.getProperty("rocksdb.memory.shard.count", "4"));
        rocksDBSubmitTimeoutMs = Long.parseLong(centralRepository.getProperty("rocksdb.submit.timeout.ms", "100"));
        rocksDBDiskShardCount = Integer.parseInt(centralRepository.getProperty("rocksdb.disk.shard.count", "64"));
        rocksdbNodeName = centralRepository.getProperty("node.name", "node-0");
        fraudManagerMultiNode = Boolean.parseBoolean(centralRepository.getProperty("fraudmanager.multinode", "false"));
        fraudManagerAlertStoringEnabled = Boolean.parseBoolean(centralRepository.getProperty("fraudmanager.alertstoring.enabled", "false"));

        String shardsProp = centralRepository.getProperty("rocksdb." + rocksdbNodeName + ".shards", "0");
        if (shardsProp != null && !shardsProp.isEmpty()) {
            String[] parts = shardsProp.split(",");
            for (String part : parts) {
                if (part.contains("-")) {
                    String[] range = part.split("-");
                    int start = Integer.parseInt(range[0].trim());
                    int end = Integer.parseInt(range[1].trim());
                    for (int i = start; i <= end; i++) {
                        rocksDBShards.add(i);
                    }
                } else {
                    rocksDBShards.add(Integer.parseInt(part.trim()));
                }
            }
            logger.info("Loaded shards for node {}: {}", rocksdbNodeName, rocksDBShards);
        } else {
            logger.warn("No shards configuration found for node: {}", rocksdbNodeName);
        }
    }

    public NatsService natsService() {
        try {
            Connection nc = Nats.connect(NATS_PROTOCOL + natsHost + ":" + natsPort);
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
            RocksDBService rocksDBService = rocksDBService(rocksDBPath, rocksDBQueueSize);
            FraudProcessor fraudProcessor = new FraudProcessor(rocksDBService, nc);
            QueryProcessor queryProcessor = new QueryProcessor(rocksDBService, nc);
            return new NatsService(nc, executor, fraudProcessor, queryProcessor, rocksDBService);
        } catch (Exception e) {
            throw new RuntimeException("Error creating NatsService", e);
        }
    }

    public RocksDBService rocksDBService(String rocksDBPath, int rocksDBQueueSize) {
        return new RocksDBService(rocksDBPath, rocksDBQueueSize);
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

package ma.s2m.fraudmanager.config;

import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.s2m.fraudmanager.service.FraudProcessor;
import ma.s2m.fraudmanager.service.NatsService;
import ma.s2m.fraudmanager.service.RedisService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private final Properties props;
    private static final String NATS_PROTOCOL = "nats://";
    public static String natsHost;
    public static int natsPort;
    public static String redisHost;
    public static int redisPort;
    public static String redisUser;
    public static String redisPassword;
    public static String natsTopic;
    public static String redisDatabase;
    public static int appQueueCapacity;
    public static int appThreadPoolSize;
    public static String ruleDeploymentDir;
    public static Boolean droolsDebugEnabled = false;
    public static Boolean droolsProfilerEnabled = false;
    public static Boolean droolsRulesAgendaGroupRuleTypeEnabled = false;
    public static String appProcessorMessagingProvider = "";

    public AppConfig() {
        props = new Properties();
        try (InputStream is = getClass().getResourceAsStream("/application.properties")) {
            props.load(is);

            natsHost = props.getProperty("nats.host", "localhost");
            if (System.getenv("NATS_HOST") != null) {
                natsHost = System.getenv("NATS_HOST");
            }
            natsPort = Integer.parseInt(props.getProperty("nats.port", "4222"));
            if (System.getenv("NATS_PORT") != null) {
                natsPort = Integer.parseInt(System.getenv("NATS_PORT"));
            }
            natsTopic = props.getProperty("nats.topic", "fraud.check");
            if (System.getenv("NATS_TOPIC") != null) {
                natsTopic = System.getenv("NATS_TOPIC");
            }
            redisHost = props.getProperty("redis.host", "localhost");
            if (System.getenv("REDIS_HOST") != null) {
                redisHost = System.getenv("REDIS_HOST");
            }
            redisPort = Integer.parseInt(props.getProperty("redis.port", "6379"));
            if (System.getenv("REDIS_PORT") != null) {
                redisPort = Integer.parseInt(System.getenv("REDIS_PORT"));
            }
            redisDatabase = props.getProperty("redis.database", "0");
            if (System.getenv("REDIS_DATABASE") != null) {
                redisDatabase = System.getenv("REDIS_DATABASE");
            }
            redisUser = props.getProperty("redis.user", "default");
            if (System.getenv("REDIS_USER") != null) {
                redisUser = System.getenv("REDIS_USER");
            }
            redisPassword = props.getProperty("redis.password", "");
            if (System.getenv("REDIS_PASSWORD") != null) {
                redisPassword = System.getenv("REDIS_PASSWORD");
            }
            appQueueCapacity = Integer.parseInt(props.getProperty("app.queue.capacity", "1000"));
            if (System.getenv("APP_QUEUE_CAPACITY") != null) {
                appQueueCapacity = Integer.parseInt(System.getenv("APP_QUEUE_CAPACITY"));
            }
            appThreadPoolSize = Integer.parseInt(props.getProperty("app.thread.pool.size", "20"));
            if (System.getenv("APP_THREAD_POOL_SIZE") != null) {
                appThreadPoolSize = Integer.parseInt(System.getenv("APP_THREAD_POOL_SIZE"));
            }
            ruleDeploymentDir = props.getProperty("drools.rules.deployment.dir");
            if (System.getenv("DROOLS_RULES_DEPLOYMENT_DIR") != null) {
                ruleDeploymentDir = System.getenv("DROOLS_RULES_DEPLOYMENT_DIR");
            }
            if (ruleDeploymentDir == null || ruleDeploymentDir.isEmpty()) {
                ruleDeploymentDir = "./rules";
            }
            droolsDebugEnabled = Boolean.parseBoolean(props.getProperty("drools.debug.enabled", "false"));
            if (System.getenv("DROOLS_DEBUG_ENABLED") != null) {
                droolsDebugEnabled = Boolean.parseBoolean(System.getenv("DROOLS_DEBUG_ENABLED"));
            }
            droolsProfilerEnabled = Boolean.parseBoolean(props.getProperty("drools.profiler.enabled", "false"));
            if (System.getenv("DROOLS_PROFILER_ENABLED") != null) {
                droolsProfilerEnabled = Boolean.parseBoolean(System.getenv("DROOLS_PROFILER_ENABLED"));
            }
            droolsRulesAgendaGroupRuleTypeEnabled = Boolean.parseBoolean(props.getProperty("drools.rules.agenda-group.ruletype.enabled", "false"));
            if (System.getenv("DROOLS_RULES_AGENDA_GROUP_RULE_TYPE_ENABLED") != null) {
                droolsRulesAgendaGroupRuleTypeEnabled = Boolean.parseBoolean(System.getenv("DROOLS_RULES_AGENDA_GROUP_RULE_TYPE_ENABLED"));
            }
            appProcessorMessagingProvider = props.getProperty("app.processor.messaging.provider", "NATS");
            if (System.getenv("APP_PROCESSOR_MESSAGING_PROVIDER") != null) {
                appProcessorMessagingProvider = System.getenv("APP_PROCESSOR_MESSAGING_PROVIDER");
            }

        } catch (Exception e) {
            logger.error("Error loading properties", e);
            throw new RuntimeException(e);
        }
        
    }

    public NatsService natsService() {
        try {
            Connection nc = Nats.connect(NATS_PROTOCOL + natsHost + ":" + natsPort);
            BlockingQueue<Message> queue = new ArrayBlockingQueue<>(appQueueCapacity);
            ExecutorService executor = Executors.newFixedThreadPool(appThreadPoolSize);
            FraudProcessor processor = new FraudProcessor(redisService(), nc);
            return new NatsService(nc, queue, executor, processor, props);
        } catch (Exception e) {
            throw new RuntimeException("Error creating NatsService", e);
        }
    }

    public RedisService redisService() {
        RedisURI redisUri = RedisURI.builder()
            .withHost(AppConfig.redisHost)
            .withPort(AppConfig.redisPort)
            .withAuthentication(AppConfig.redisUser, AppConfig.redisPassword)
            .withDatabase(0) // Spécifie la base de données 0
            .build();
        RedisClient client = RedisClient.create(redisUri);
        return new RedisService(client);
    }

    private static String ruleTypeConverter(int ruleType) {
        switch (ruleType) {
            case RuleDefinition.RULE_TYPE_ALERT : return "ALERT";
            case RuleDefinition.RULE_TYPE_COMPUTE : return "COMPUTE";
            default : throw new IllegalArgumentException("Unsupported rule type: " + ruleType);
        }
    }

    public static String ruleTypePrefix(int ruleType) {
        return droolsRulesAgendaGroupRuleTypeEnabled ? ruleTypeConverter(ruleType) + ":" : "";
    }

}

package ma.s2m.fraudmanager;

import ma.medtech.droolbuilder.publisher.providers.DroolBuilderRuleProviderFactory;
import ma.medtech.droolbuilder.publisher.providers.IDroolBuilderRuleProvider;
import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.config.RulesConfig;
import ma.s2m.fraudmanager.service.NatsService;
import ma.s2m.fraudmanager.service.RedisService;
import ma.s2m.fraudmanager.util.Subject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static NatsService natsService;

    public static void main(String[] args) { 
        addShutdownHook();

        try {
            AppConfig config = new AppConfig();
            init();  // Initialisation des règles avant de démarrer le service NATS
            natsService = config.natsService();
            natsService.startConsumer();  // Démarre le consumer NATS et les workers
            logger.info("Application started. Listening for transactions...");
            // Blocage pour garder l'app en vie en permanence
            Thread.currentThread().join();
        } catch (Exception e) {
            logger.error("Error starting application", e);
        }
    }

    public static void init() {
        IDroolBuilderRuleProvider ruleProvider = DroolBuilderRuleProviderFactory.getRuleProvider(IDroolBuilderRuleProvider.PROVIDER_TYPE_DB_REDIS);
        String deployedVersion=null;
        Boolean deployed = false;

        while(!deployed) {
            try {
                deployedVersion = ruleProvider.getCurrentlyDeployed();
                if (deployedVersion == null) {
                    logger.error("No ruleset deployed, waiting for 5 minutes before retrying");
                    Thread.sleep(300000);
                }
                else {
                    deployed = true;
                    logger.info("Deployed ruleset version: " + deployedVersion);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("Using deployed ruleset version: " + deployedVersion + " from " + AppConfig.ruleDeploymentDir);
        String deployedRuleset = deployedVersion.split(":")[0];
        String deployedVersionNumber = deployedVersion.split(":")[1];
        String versionToDeploy = deployedRuleset + "-" + deployedVersionNumber;

        List<RuleDefinition> rules = null;

        rules = ruleProvider.fetchRulesByRuleSetId(deployedRuleset, versionToDeploy);

        if (rules == null || rules.size() == 0) {
            throw new RuntimeException("No rules found");
        }

        RulesConfig.extendedVersion = versionToDeploy;
        logger.debug("Total rules fetched: " + rules.size());
        
        // Count alert rules separately before processing the lambda
        Integer alertRulesCount = 0;
        for (RuleDefinition ruleDefinition : rules) {
            if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                alertRulesCount++;
            }
        }

        logger.debug("Total alert rules: " + alertRulesCount);

        RulesConfig.alertRulesCount = alertRulesCount;
        RulesConfig.rulesMapForCardSubject = new HashMap<>();
        RulesConfig.rulesMapForMerchantSubject = new HashMap<>();
        RulesConfig.rulesMapForAnySubject = new HashMap<>();
        RulesConfig.rulesMapForCustomSubject = new HashMap<>();

        rules.forEach(ruleDefinition -> {
            Long windowSize = ruleDefinition.getTimeFrame() * ruleDefinition.getTimeframeUnit();

            if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.CARD)) {
                List<RuleDefinition> ruleDefinitions = RulesConfig.rulesMapForCardSubject.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                RulesConfig.rulesMapForCardSubject.put(windowSize, ruleDefinitions);
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.MERCHANT)) {
                List<RuleDefinition> ruleDefinitions = RulesConfig.rulesMapForMerchantSubject.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                RulesConfig.rulesMapForMerchantSubject.put(windowSize, ruleDefinitions);
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.ANY)) {
                List<RuleDefinition> ruleDefinitions = RulesConfig.rulesMapForAnySubject.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                RulesConfig.rulesMapForAnySubject.put(windowSize, ruleDefinitions);
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.CUSTOM)) {

                String customSubjectKey = ruleDefinition.getCustomSubject();
                HashMap<Long, List<RuleDefinition>> customWindows = RulesConfig.rulesMapForCustomSubject.get(customSubjectKey);
                if (customWindows == null) {
                    customWindows = new HashMap<>();
                }
                List<RuleDefinition> ruleDefinitions = customWindows.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                customWindows.put(windowSize, ruleDefinitions);
                RulesConfig.rulesMapForCustomSubject.put(customSubjectKey, customWindows);
            } else {
                logger.warn("Unknown subject type: " + ruleDefinition.getSubject() + " for rule: " + ruleDefinition.getRuleTitle());
            }

        });

        RulesConfig.cardSubjectPresent = !RulesConfig.rulesMapForCardSubject.isEmpty();
        RulesConfig.merchantSubjectPresent = !RulesConfig.rulesMapForMerchantSubject.isEmpty();
        RulesConfig.anySubjectPresent = !RulesConfig.rulesMapForAnySubject.isEmpty();
        RulesConfig.customSubjectPresent = !RulesConfig.rulesMapForCustomSubject.isEmpty();
    }

    /**
     * Ajoute un shutdown hook pour supprimer toutes les clés commençant par "Card:" ou "Merchant"
     */
    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (!AppConfig.appRedisCleanOnShutdown) {
                    logger.info("Redis cleanup on shutdown is disabled. Skipping cleanup.");
                    return;
                }
                logger.info("Intercepting shutdown signal. Cleaning up Redis keys...");
                AppConfig config = new AppConfig();
                RedisService redisService = config.redisService();
                // Supprimer les clés commençant par "Card:"
                List<String> keys = redisService.getKeysByPattern("Card:*");
                if (keys != null) {
                    keys.forEach(redisService::deleteKey);
                }
                keys = redisService.getKeysByPattern("Merchant:*");
                if (keys != null) {
                    keys.forEach(redisService::deleteKey);
                }
                keys = redisService.getKeysByPattern("Custom:*");
                if (keys != null) {
                    keys.forEach(redisService::deleteKey);
                }
                keys = redisService.getKeysByPattern("lock:*");
                if (keys != null) {
                    keys.forEach(redisService::deleteKey);
                }
                logger.info("Redis cleanup completed.");
                natsService.stop();
            } catch (Exception e) {
                logger.error("Error during Redis cleanup on shutdown", e);
            }
        }));
    }

}
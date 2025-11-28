package ma.s2m.fraudmanager;

import ma.medtech.droolbuilder.publisher.providers.DroolBuilderRuleProviderFactory;
import ma.medtech.droolbuilder.publisher.providers.IDroolBuilderRuleProvider;
import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.medtech.droolbuilder.utils.Utils;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.config.RulesConfig;
import ma.s2m.fraudmanager.service.FraudProcessor;
import ma.s2m.fraudmanager.service.NatsService;
import ma.s2m.fraudmanager.service.RocksDBService;
import ma.s2m.fraudmanager.util.Subject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static NatsService natsService;

    public static void main(String[] args) {
        addShutdownHook();

        try {
            AppConfig config = new AppConfig();
            init(); // Initialisation des règles avant de démarrer le service NATS
            natsService = config.natsService();
            natsService.startConsumer(); // Démarre le consumer NATS et les workers
            logger.info("Application started. Listening for transactions...");
            // Blocage pour garder l'app en vie en permanence
            Thread.currentThread().join();
        } catch (Exception e) {
            logger.error("Error starting application", e);
        }
    }

    public static void init() {
        IDroolBuilderRuleProvider ruleProvider = DroolBuilderRuleProviderFactory
                .getRuleProvider(IDroolBuilderRuleProvider.PROVIDER_TYPE_DB_REDIS);
        String deployedVersion = null;
        Boolean deployed = false;

        while (!deployed) {
            try {
                deployedVersion = ruleProvider.getCurrentlyDeployed();
                if (deployedVersion == null) {
                    logger.error("No ruleset deployed, waiting for 5 minutes before retrying");
                    Thread.sleep(AppConfig.waitTime);
                } else {
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
        String currentlyDeployedVersion = deployedRuleset + "-" + deployedVersionNumber;

        List<RuleDefinition> rules = null;

        rules = ruleProvider.fetchRulesByRuleSetId(deployedRuleset, currentlyDeployedVersion);

        if (rules == null || rules.size() == 0) {
            throw new RuntimeException("No rules found");
        }

        RulesConfig.allrules = rules;
        RulesConfig.extendedVersion = currentlyDeployedVersion;
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
        RulesConfig.rulesMapForCustomSubject = new HashMap<>();

        rules.forEach(ruleDefinition -> {
            Long windowSize = ruleDefinition.getTimeFrame() * ruleDefinition.getTimeframeUnit();

            if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.CARD) && !ruleDefinition.getFixedWindow()) {
                List<RuleDefinition> ruleDefinitions = RulesConfig.rulesMapForCardSubject.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                RulesConfig.rulesMapForCardSubject.put(windowSize, ruleDefinitions);
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.MERCHANT)
                    && !ruleDefinition.getFixedWindow()) {
                List<RuleDefinition> ruleDefinitions = RulesConfig.rulesMapForMerchantSubject.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                RulesConfig.rulesMapForMerchantSubject.put(windowSize, ruleDefinitions);
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.CUSTOM) && !ruleDefinition.getFixedWindow()) {
                String customSubject = ruleDefinition.getCustomSubject();
                String customSubjectKey = customSubject;
                HashMap<Long, List<RuleDefinition>> customWindows = RulesConfig.rulesMapForCustomSubject
                        .get(customSubjectKey);
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
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.CUSTOM) && ruleDefinition.getFixedWindow()) {
                String customSubject = ruleDefinition.getCustomSubject();
                String customSubjectKey = customSubject;
                HashMap<Long, List<RuleDefinition>> customWindows = RulesConfig.rulesMapForCustomSubjectFixedWindow.get(customSubjectKey);
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
                RulesConfig.rulesMapForCustomSubjectFixedWindow.put(customSubjectKey, customWindows);
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.CARD) && ruleDefinition.getFixedWindow()) {
                List<RuleDefinition> ruleDefinitions = RulesConfig.rulesMapForCardSubjectFixedWindow.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                RulesConfig.rulesMapForCardSubjectFixedWindow.put(windowSize, ruleDefinitions);               
            } else if (ruleDefinition.getSubject().equalsIgnoreCase(Subject.MERCHANT) && ruleDefinition.getFixedWindow()) {
                List<RuleDefinition> ruleDefinitions = RulesConfig.rulesMapForMerchantSubjectFixedWindow.get(windowSize);
                if (ruleDefinitions == null) {
                    ruleDefinitions = new ArrayList<>();
                }
                if (ruleDefinition.getRuleType() == RuleDefinition.RULE_TYPE_ALERT) {
                    ruleDefinitions.add(ruleDefinition);
                }
                RulesConfig.rulesMapForMerchantSubjectFixedWindow.put(windowSize, ruleDefinitions);
            } else {
                logger.warn("Unknown subject type: " + ruleDefinition.getSubject() + " for rule: "
                        + ruleDefinition.getRuleTitle());
            }

        });

        RulesConfig.cardSubjectPresent = !RulesConfig.rulesMapForCardSubject.isEmpty();
        RulesConfig.cardSubjectFixedWindowPresent = !RulesConfig.rulesMapForCardSubjectFixedWindow.isEmpty();
        RulesConfig.merchantSubjectPresent = !RulesConfig.rulesMapForMerchantSubject.isEmpty();
        RulesConfig.merchantSubjectFixedWindowPresent = !RulesConfig.rulesMapForMerchantSubjectFixedWindow.isEmpty();
        RulesConfig.customSubjectPresent = !RulesConfig.rulesMapForCustomSubject.isEmpty();
        RulesConfig.customSubjectFixedWindowPresent = !RulesConfig.rulesMapForCustomSubjectFixedWindow.isEmpty();

        RulesConfig.cardSubjectSize = RulesConfig.rulesMapForCardSubject.size();
        RulesConfig.merchantSubjectSize = RulesConfig.rulesMapForMerchantSubject.size();
        RulesConfig.customSubjectSize = RulesConfig.rulesMapForCustomSubject.size();

        RulesConfig.rulesMapArrayForCardSubject = getRulesMapArray(RulesConfig.rulesMapForCardSubject);
        RulesConfig.rulesMapArrayForMerchantSubject = getRulesMapArray(RulesConfig.rulesMapForMerchantSubject);

        rules.forEach(rule -> {
            String cleanedGroupName = Utils.cleanGroupName(rule.getGroup());
            RulesConfig.ruleGroupSet.add(cleanedGroupName);
            Long windowSize = rule.getTimeFrame() * rule.getTimeframeUnit();
            String subjectKey = Subject.CUSTOM.equals(rule.getSubject()) ? rule.getSubject() + FraudProcessor.KEY_SEPARATOR + rule.getCustomSubject() : rule.getSubject();
            String windowKey = subjectKey + FraudProcessor.WINDOW_SEPARATOR + windowSize;
            Set<String> groupSet = RulesConfig.ruleGroupsPerWindowSizeMap.get(windowKey);
            if (groupSet == null) {
                groupSet = new HashSet<>();
            }
            groupSet.add(cleanedGroupName);
            RulesConfig.ruleGroupsPerWindowSizeMap.put(windowKey, groupSet);
        });
    }

    private static List<HashMap<Long, List<RuleDefinition>>> getRulesMapArray(
            HashMap<Long, List<RuleDefinition>> rulesMap) {
        List<HashMap<Long, List<RuleDefinition>>> allMaps = new ArrayList<>();
        if (rulesMap == null || rulesMap.isEmpty()) {
            return allMaps;
        }
        if (rulesMap.size() <= AppConfig.appProcessorSubjectParallelismThreshold) {
            allMaps.add(rulesMap);
            return allMaps;
        }
        // Split the rulesMap into two maps for parallel processing
        HashMap<Long, List<RuleDefinition>> map1 = new HashMap<>();
        HashMap<Long, List<RuleDefinition>> map2 = new HashMap<>();
        int index = 0;
        for (Map.Entry<Long, List<RuleDefinition>> entry : rulesMap.entrySet()) {
            if (index % 2 == 0) {
                map1.put(entry.getKey(), entry.getValue());
            } else {
                map2.put(entry.getKey(), entry.getValue());
            }
            index++;
        }
        allMaps.add(map1);
        allMaps.add(map2);
        return allMaps;
    }

    /**
     * Ajoute un shutdown hook pour supprimer toutes les clés commençant par "Card:"
     * ou "Merchant"
     */
    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (!AppConfig.appRocksDBCleanOnShutdown) {
                    logger.info("RocksDB cleanup on shutdown is disabled. Skipping cleanup.");
                    return;
                }
                logger.info("Intercepting shutdown signal. Cleaning up RocksDB keys...");
                AppConfig config = new AppConfig();
                RocksDBService rocksDBService = config.rocksDBService(AppConfig.rocksDBPath,
                        AppConfig.rocksDBQueueSize);
                // Supprimer les clés commençant par "Card:"
                List<String> keys = rocksDBService.getKeysByPattern("Card:*");
                if (keys != null) {
                    keys.forEach(rocksDBService::deleteKey);
                }
                // Supprimer les clés commençant par "Merchant:"
                keys = rocksDBService.getKeysByPattern("Merchant:*");
                if (keys != null) {
                    keys.forEach(rocksDBService::deleteKey);
                }
                // Supprimer les clés commençant par "Custom:"
                keys = rocksDBService.getKeysByPattern("Custom:*");
                if (keys != null) {
                    keys.forEach(rocksDBService::deleteKey);
                }
                // Supprimer les clés commençant par "lock:"
                keys = rocksDBService.getKeysByPattern("lock:*");
                if (keys != null) {
                    keys.forEach(rocksDBService::deleteKey);
                }
                logger.info("RocksDB cleanup completed.");
                natsService.stop();
            } catch (Exception e) {
                logger.error("Error during RocksDB cleanup on shutdown", e);
            }
        }));
    }

}
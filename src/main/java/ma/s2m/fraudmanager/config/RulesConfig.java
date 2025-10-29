package ma.s2m.fraudmanager.config;

import java.util.HashMap;
import java.util.List;

import ma.medtech.droolbuilder.rules.RuleDefinition;

public class RulesConfig {
    public static HashMap<Long, List<RuleDefinition>> rulesMapForCardSubject = new HashMap<>();
    public static Boolean cardSubjectPresent = false;
    public static HashMap<Long, List<RuleDefinition>> rulesMapForMerchantSubject = new HashMap<>();
    public static Boolean merchantSubjectPresent = false;
    public static HashMap<Long, List<RuleDefinition>> rulesMapForAnySubject = new HashMap<>();
    public static Boolean anySubjectPresent = false;
    public static HashMap<String, HashMap<Long, List<RuleDefinition>>> rulesMapForCustomSubject = new HashMap<>();
    public static Boolean customSubjectPresent = false;
    public static Integer alertRulesCount = 0;
    public static String extendedVersion = "";

}

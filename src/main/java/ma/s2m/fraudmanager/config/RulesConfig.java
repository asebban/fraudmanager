package ma.s2m.fraudmanager.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import ma.medtech.droolbuilder.rules.RuleDefinition;

public class RulesConfig {
    public static HashMap<Long, List<RuleDefinition>> rulesMapForCardSubject = new HashMap<>();
    public static Boolean cardSubjectPresent = false;
    public static Integer cardSubjectSize = 0;
    public static List<HashMap<Long, List<RuleDefinition>>> rulesMapArrayForCardSubject = new ArrayList<>();

    public static HashMap<Long, List<RuleDefinition>> rulesMapForMerchantSubject = new HashMap<>();
    public static Boolean merchantSubjectPresent = false;
    public static Integer merchantSubjectSize = 0;
    public static List<HashMap<Long, List<RuleDefinition>>> rulesMapArrayForMerchantSubject = new ArrayList<>();

    public static HashMap<Long, List<RuleDefinition>> rulesMapForAnySubject = new HashMap<>();
    public static Boolean anySubjectPresent = false;
    public static Integer anySubjectSize = 0;
    public static List<HashMap<Long, List<RuleDefinition>>> rulesMapArrayForAnySubject = new ArrayList<>();

    public static HashMap<String, HashMap<Long, List<RuleDefinition>>> rulesMapForCustomSubject = new HashMap<>();
    public static Boolean customSubjectPresent = false;
    public static Integer customSubjectSize = 0;
    public static List<HashMap<String, HashMap<Long, List<RuleDefinition>>>> rulesMapArrayForCustomSubject = new ArrayList<>();

    public static List<String> ruleGroupList = new ArrayList<>();
    public static Integer alertRulesCount = 0;
    public static String extendedVersion = "";

}

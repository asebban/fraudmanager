package ma.s2m.fraudmanager.drools;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;

import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;
import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.medtech.droolbuilder.utils.DurationFormatter;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.drools.listeners.RuleProfiler;
import ma.s2m.fraudmanager.model.Measurment;

final class StatefulDroolsSession implements DroolsSession {
    private final KieSession ks;
    private String subject;
    private Long windowSize = 0L;
    private Boolean noRules = false;
    private Logger logger = org.slf4j.LoggerFactory.getLogger(StatefulDroolsSession.class);
    private Boolean droolsProfilerEnabled = AppConfig.droolsProfilerEnabled;

    StatefulDroolsSession(KieSession ks) { this.ks = ks; }

    @Override public void setGlobal(String name, Object value) { ks.setGlobal(name, value); }

    @Override public void execute(Object... facts) {
        
        Long beginExecuteDrools = System.nanoTime();

        if (noRules) {
            logger.debug("###### No rules executed, skipping processing");
            return;
        }

        Measurment m=null;
        if (facts != null) for (Object f : facts) {
            if (f instanceof Measurment) {
                m = (Measurment) f;
                ks.insert(f);
            }
            if (f instanceof Long) {
                windowSize = (Long) f;
            }
            if (f instanceof String) {
                subject = (String) f;
            }
        }

        Duration duration = Duration.ofMillis(windowSize);
        String formattedDuration = DurationFormatter.formatDuration(duration);

        RuleProfiler ruleProfiler = null;
        if (droolsProfilerEnabled) {
            ruleProfiler = (RuleProfiler) getAgendaEventListener();
            if (ruleProfiler != null) ruleProfiler.reset();
        }

        if (AppConfig.droolsRulesAgendaGroupRuleTypeEnabled) {
            ks.getAgenda().getAgendaGroup(AppConfig.ruleTypePrefix(RuleDefinition.RULE_TYPE_ALERT) + this.subject + "->" + formattedDuration).setFocus();
            ks.getAgenda().getAgendaGroup(AppConfig.ruleTypePrefix(RuleDefinition.RULE_TYPE_COMPUTE) + this.subject + "->" + formattedDuration).setFocus();
            Long t0 = System.nanoTime();
            logger.debug("###### Begin internal ksession execute drools to before fireAllRules for trx {} and window {} is {} ms", m != null ? m.getTransaction().getTransactionNo() : "N/A", formattedDuration, (t0 - beginExecuteDrools)/1_000_000);
            ks.fireAllRules();
            Long t1 = System.nanoTime();
            logger.debug("###### Rules fired in " + (t1 - t0)/1_000_000 + " ms for trx " + (m != null && m.getTransaction() != null ? m.getTransaction().getTransactionNo() : "N/A"));

            if (droolsProfilerEnabled) {
                System.out.printf("********** Drools Rule Profiling Report for trx %s and window %s:\n", m != null ? m.getTransaction().getTransactionNo() : "N/A", formattedDuration);
                if (ruleProfiler != null) ruleProfiler.reportTop(10).forEach(System.out::println);
            }
            
            logger.debug("###### Time of internal logging ksession execute drools after fireAllRules for trx {} and window {} is {} ms", m != null ? m.getTransaction().getTransactionNo() : "N/A", formattedDuration, (System.nanoTime() - t1)/1_000_000);
       } else {
            ks.getAgenda().getAgendaGroup(this.subject + "->" + formattedDuration).setFocus();
            Long t0 = System.nanoTime();
            ks.fireAllRules();
            logger.debug("###### Rules fired in " + (System.nanoTime() - t0)/1_000_000 + " ms for trx " + (m != null && m.getTransaction() != null ? m.getTransaction().getTransactionNo() : "N/A"));

            if (droolsProfilerEnabled) {
                if (ruleProfiler != null) ruleProfiler.reportTop(10).forEach(logger::debug);
            }
        }
        clean();
    }

    @Override public void close() { ks.dispose(); }

    @Override public void clean() { 
        for (Iterator<FactHandle> iterator = ks.getFactHandles().iterator(); iterator.hasNext(); ) {
            FactHandle factHandle = iterator.next();
            ks.delete(factHandle);
        }
    }

    // Optionnel : exposer KieSession pour ajouter listeners, channels…
    public KieSession unwrap() { return ks; }

    @Override
    public void addEventListener(AgendaEventListener eventListener) {
        ks.addEventListener(eventListener);
    }

    @Override
    public void removeEventListener(AgendaEventListener eventListener) {
        ks.removeEventListener(eventListener);
    }

    @Override
    public AgendaEventListener getAgendaEventListener() {
        Collection<AgendaEventListener> listeners = ks.getAgendaEventListeners();
        if (listeners != null && !listeners.isEmpty()) {
            for (AgendaEventListener listener : listeners) {
                if (listener instanceof RuleProfiler) {
                    return listener;
                }
            }
        }
        return null;
    }

    @Override
    public void dispose() {
        ks.dispose();
    }

    

}

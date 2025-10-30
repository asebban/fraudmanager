package ma.s2m.fraudmanager.drools;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;
import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.drools.listeners.RuleProfiler;
import ma.s2m.fraudmanager.model.Measurment;

final class StatefulDroolsSession implements DroolsSession {
    private final KieSession ks;
    private String subject;
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
            if (f instanceof List<?>) {
                List<?> list = (List<?>) f;
                for (Object item : list) {
                    if (item instanceof Measurment) {
                        ks.insert(item);
                    }
                }
            }
            if (f instanceof String) {
                subject = (String) f;
            }
        }

        RuleProfiler ruleProfiler = null;
        if (droolsProfilerEnabled) {
            ruleProfiler = (RuleProfiler) getAgendaEventListener();
            if (ruleProfiler != null) ruleProfiler.reset();
        }

        if (AppConfig.droolsRulesAgendaGroupRuleTypeEnabled) {
            ks.getAgenda().getAgendaGroup(AppConfig.ruleTypePrefix(RuleDefinition.RULE_TYPE_ALERT) + this.subject).setFocus();
            ks.getAgenda().getAgendaGroup(AppConfig.ruleTypePrefix(RuleDefinition.RULE_TYPE_COMPUTE) + this.subject).setFocus();
            Long t0 = System.nanoTime();
            logger.debug("Time {} Internal ksession execute drools to before fireAllRules for subject {}", (t0 - beginExecuteDrools)/1_000_000, subject);
            ks.fireAllRules();
            Long t1 = System.nanoTime();
            logger.debug("Time {} ms of execution of fireAllRules for subject {}", (t1 - t0)/1_000_000, subject);
            if (droolsProfilerEnabled) {
                System.out.printf("********** Drools Rule Profiling Report for trx %s and subject %s:\n", m != null ? m.getTransaction().getTransactionNo() : "N/A", subject);
                if (ruleProfiler != null) ruleProfiler.reportTop(10).forEach(System.out::println);
            }
            
       } else {
            ks.getAgenda().getAgendaGroup(this.subject).setFocus();
            Long t0 = System.nanoTime();
            ks.fireAllRules();
            logger.debug("Time {} ms of execution of fireAllRules for subject {}", (System.nanoTime() - t0)/1_000_000, subject);

            if (droolsProfilerEnabled) {
                if (ruleProfiler != null) ruleProfiler.reportTop(10).forEach(logger::debug);
            }
        }
        Long cleanTimeStart = System.nanoTime();
        this.clean();
        Long cleanTimeEnd = System.nanoTime();
        logger.debug("Time {} ms of cleaning after rules execution for subject {}", (cleanTimeEnd - cleanTimeStart)/1_000_000, subject);
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

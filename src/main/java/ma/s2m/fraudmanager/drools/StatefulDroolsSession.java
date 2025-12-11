package ma.s2m.fraudmanager.drools;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.EntryPoint;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;

import ma.medtech.droolbuilder.rules.Subject;
import ma.medtech.droolbuilder.utils.DurationFormatter;
import ma.s2m.fraudmanager.config.AppConfig;
import ma.s2m.fraudmanager.config.RulesConfig;
import ma.s2m.fraudmanager.drools.listeners.RuleProfiler;
import ma.s2m.fraudmanager.model.Measurment;
import ma.s2m.fraudmanager.service.processors.FraudProcessor;

final class StatefulDroolsSession implements DroolsSession {
    private final KieSession ks;
    private String extendedSubject;
    private Boolean noRules = false;
    private Logger logger = org.slf4j.LoggerFactory.getLogger(StatefulDroolsSession.class);
    private Boolean droolsProfilerEnabled = AppConfig.droolsProfilerEnabled;
    private Map<String, EntryPoint> entryPointsMap = new HashMap<>();
    private Map<FactHandle, EntryPoint> insertedHandles = new HashMap<>();
    private RuleProfiler ruleProfiler = new RuleProfiler();
    private String correlationId = "";

    public StatefulDroolsSession(KieSession ks) {
        this.ks = ks;
        for (String groupName : RulesConfig.ruleGroupSet) {
            EntryPoint entryPoint = ks.getEntryPoint(groupName);
            if (entryPoint != null) {
                entryPointsMap.put(groupName, entryPoint);
            }
        }
        if (droolsProfilerEnabled) {
            this.ks.addEventListener(this.ruleProfiler);
        }

    }

    @Override
    public void setGlobal(String name, Object value) {
        ks.setGlobal(name, value);
    }

    @Override
    public void execute(Object... facts) {

        if (noRules) {
            logger.debug("###### No rules executed, skipping processing");
            return;
        }

        Measurment m = null;
        if (facts != null)
            for (Object f : facts) {
                if (f instanceof Measurment) {
                    m = (Measurment) f;
                }
                if (f instanceof String) {
                    String s = (String) f;
                    if (s != null && !s.equals("") && !s.equals(Subject.CARD) && !s.equals(Subject.MERCHANT)
                            && !s.startsWith(Subject.CUSTOM)) {
                        this.correlationId = s;
                    } else {
                        this.extendedSubject = s;
                    }
                }
            }

        Duration duration = Duration.ofMillis(m.getWindowSize());
        String formattedDuration = DurationFormatter.formatDuration(duration);

        if (droolsProfilerEnabled) {
            this.ruleProfiler.reset();
        }

        Set<String> groupSet = RulesConfig.ruleGroupsPerWindowSizeMap.get(this.extendedSubject + FraudProcessor.WINDOW_SEPARATOR + m.getWindowSize());
        if (groupSet != null) {
            for (String groupName : groupSet) {
                EntryPoint ep = this.entryPointsMap.get(groupName);
                if (ep != null) {
                    FactHandle fh = ep.insert(m);
                    insertedHandles.put(fh, ep);
                } else {
                    logger.warn("EntryPoint not found for group name: {}", groupName);
                }
            }
        }

        if (AppConfig.droolsRulesAgendaGroupRuleTypeEnabled) {
            Long t0 = System.currentTimeMillis();
            ks.fireAllRules();
            Long t1 = System.currentTimeMillis();
            logger.debug("Time {} ms [{}] [{}] trx={} key={} ms of execution of fireAllRules for window {}, last rule name: {}", (t1 - t0),
                    this.correlationId, this.extendedSubject, m != null ? m.getTransaction().getTransactionNo() : "N/A",
                    m != null ? m.getKey() : "N/A", formattedDuration, this.ruleProfiler.getLastRuleName());
            if (droolsProfilerEnabled) {
                final Measurment finalM = m;
                final String cId = this.correlationId;
                this.ruleProfiler.reportTop(10).forEach(s -> {
                    logger.debug("[{}] trx {} - win {} : {} : {}", cId,
                            finalM != null ? finalM.getTransaction().getTransactionNo() : "N/A", formattedDuration,
                            this.extendedSubject, s);
                });
            }

        } else {
            Long t0 = System.nanoTime();
            ks.fireAllRules();
            logger.debug("Time {} ms of execution of fireAllRules for window {}, trx={}, subject={}",
                    (System.nanoTime() - t0) / 1_000_000, formattedDuration,
                    m != null ? m.getTransaction().getTransactionNo() : "N/A", this.extendedSubject);

            if (droolsProfilerEnabled) {
                this.ruleProfiler.reportTop(10).forEach(logger::debug);
            }
        }
        cleanEntryPoints();
    }

    private void cleanEntryPoints() {
        for (Map.Entry<FactHandle, EntryPoint> entry : insertedHandles.entrySet()) {
            FactHandle handle = entry.getKey();
            EntryPoint ep = entry.getValue();
            if (handle != null) {
                ep.delete(handle);
            }
        }
        insertedHandles.clear();
    }

    @Override
    public void close() {
        ks.dispose();
    }

    @Override
    public void clean() {
        for (Iterator<FactHandle> iterator = ks.getFactHandles().iterator(); iterator.hasNext();) {
            FactHandle factHandle = iterator.next();
            ks.delete(factHandle);
        }
    }

    // Optionnel : exposer KieSession pour ajouter listeners, channels…
    public KieSession unwrap() {
        return ks;
    }

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

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getCorrelationId() {
        return this.correlationId;
    }

}

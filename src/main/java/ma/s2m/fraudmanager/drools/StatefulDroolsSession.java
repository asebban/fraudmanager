package ma.s2m.fraudmanager.drools;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private Map<String, EntryPoint> entryPointsMap = new HashMap<>();
    private Map<FactHandle, EntryPoint> insertedHandles = new HashMap<>();
    private RuleProfiler ruleProfiler = new RuleProfiler();
    private String correlationId = "";
    private final AtomicBoolean inUse = new AtomicBoolean(false);
    private boolean broken = false;
    private int usageCount = 0;
    private static final int MAX_USAGE_COUNT = 50;

    public StatefulDroolsSession(KieSession ks) {
        this.ks = ks;
        for (String groupName : RulesConfig.ruleGroupSet) {
            EntryPoint entryPoint = ks.getEntryPoint(groupName);
            if (entryPoint != null) {
                entryPointsMap.put(groupName, entryPoint);
            }
        }
        if (AppConfig.droolsProfilerEnabled) {
            this.ks.addEventListener(this.ruleProfiler);
        }

    }

    @Override
    public void setGlobal(String name, Object value) {
        ks.setGlobal(name, value);
    }

    @Override
    public void execute(Object... facts) {
        this.usageCount++;
        if (!inUse.compareAndSet(false, true)) {
            // A KieSession is not thread-safe; concurrent usage corrupts internal state.
            throw new IllegalStateException("Drools session used concurrently");
        }

        // FAIL-FAST: Verify session is clean before use
        if (ks.getFactCount() > 0) {
            throw new IllegalStateException("Session is dirty at start of execute! FactCount=" + ks.getFactCount());
        }
        for (EntryPoint ep : entryPointsMap.values()) {
            if (ep != null && ep.getFactCount() > 0) {
                throw new IllegalStateException("Session is dirty at start of execute! EntryPoint " + ep.getEntryPointId() + " has " + ep.getFactCount() + " facts.");
            }
        }

        try {

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

        if (AppConfig.droolsProfilerEnabled) {
            this.ruleProfiler.reset();
        }

        if (m == null || m.getTransaction() == null || m.getTransaction().getTransactionNo() == null) {
            logger.error("Measurment or Transaction or TransactionNo is null, skipping rule execution. correlationId={}, subject={}",
                    this.correlationId, this.extendedSubject);
            return;
        }

        // Defensive: ensure no stale handles from a previous failed execution
        insertedHandles.clear();
        
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

        try {
            if (AppConfig.droolsRulesAgendaGroupRuleTypeEnabled) {
                Long t0 = System.currentTimeMillis();
                ks.fireAllRules();
                Long t1 = System.currentTimeMillis();
                logger.debug("Time {} ms [{}] [{}] trx={} key={} ms of execution of fireAllRules for window {}, last rule name: {}", (t1 - t0),
                        this.correlationId, this.extendedSubject, m != null ? m.getTransaction().getTransactionNo() : "N/A",
                        m != null ? m.getKey() : "N/A", formattedDuration, this.ruleProfiler.getLastRuleName());
                if (AppConfig.droolsProfilerEnabled) {
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

                if (AppConfig.droolsProfilerEnabled) {
                    this.ruleProfiler.reportTop(10).forEach(logger::debug);
                }
            }
        } catch (Exception e) {
            this.broken = true;
            logger.error("Error during fireAllRules execution, correlationId={}, subject={}, trx={}, last rule {}",
                    this.correlationId, this.extendedSubject, m != null && m.getTransaction() != null ? m.getTransaction().getTransactionNo() : "N/A", this.ruleProfiler.getLastRuleName());
            e.printStackTrace();
        } finally {
            // Always remove inserted facts, even when fireAllRules fails, to prevent corrupt reuse via pool.
            cleanEntryPoints();
        }

        } finally {
            inUse.set(false);
        }
    }

    private void cleanEntryPoints() {
        try {
             ks.getAgenda().clear();
        } catch (Exception ignore) {}
        for (Map.Entry<FactHandle, EntryPoint> entry : new HashMap<>(insertedHandles).entrySet()) {
            FactHandle handle = entry.getKey();
            EntryPoint ep = entry.getValue();
            if (handle != null) {
                try {
                    ep.delete(handle);
                } catch (Exception e) {
                    logger.debug("Ignoring delete failure for FactHandle during cleanup: {}", e.toString());
                }
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
        try {
             ks.getAgenda().clear();
        } catch (Exception ignore) {}
        
        java.util.Set<String> processed = new java.util.HashSet<>();

        // 1. Clean default entry point
        // Use generic list to avoid type issues reported by user
        for (Object fh : new java.util.ArrayList<>(ks.getFactHandles())) {
            try {
                if (fh instanceof FactHandle) {
                    ks.delete((FactHandle) fh);
                }
            } catch (Exception e) {
                 throw new RuntimeException("Failed to delete fact from default entry point", e);
            }
        }
        processed.add("DEFAULT");

        // 2. Clean Cached EntryPoints
        for (EntryPoint ep : entryPointsMap.values()) {
            if (ep == null) continue;
            String id = ep.getEntryPointId();
            if (processed.contains(id)) continue;
            
            for (Object fh : new java.util.ArrayList<>(ep.getFactHandles())) {
                try {
                     if (fh instanceof FactHandle) {
                        ep.delete((FactHandle) fh);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to delete fact from entry point " + id, e);
                }
            }
            processed.add(id);
        }

        // 3. Clean any other EntryPoints returned by ks
        for (EntryPoint ep : ks.getEntryPoints()) {
            String id = ep.getEntryPointId();
            if (processed.contains(id)) continue;
            
            for (Object fh : new java.util.ArrayList<>(ep.getFactHandles())) {
                try {
                     if (fh instanceof FactHandle) {
                        ep.delete((FactHandle) fh);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to delete fact from entry point " + id, e);
                }
            }
        }
        
        // VERIFY: If session is still not empty, THROW to discard it.
        if (ks.getFactCount() > 0) {
             throw new RuntimeException("Clean Verification Failed: Default EntryPoint still has " + ks.getFactCount() + " facts after delete!");
        }
        for (EntryPoint ep : entryPointsMap.values()) {
            if (ep != null && ep.getFactCount() > 0) {
                 throw new RuntimeException("Clean Verification Failed: EntryPoint " + ep.getEntryPointId() + " still has " + ep.getFactCount() + " facts!");
            }
        }
    }

    @Override
    public boolean isBroken() {
        return broken || usageCount >= MAX_USAGE_COUNT;
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

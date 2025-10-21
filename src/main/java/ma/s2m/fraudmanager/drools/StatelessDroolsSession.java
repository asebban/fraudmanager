package ma.s2m.fraudmanager.drools;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.kie.api.KieServices;
import org.kie.api.command.Command;
import org.kie.api.command.KieCommands;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.runtime.StatelessKieSession;
import ma.medtech.droolbuilder.rules.RuleDefinition;
import ma.medtech.droolbuilder.utils.DurationFormatter;
import ma.medtech.droolbuilder.utils.Utils;
import ma.s2m.fraudmanager.drools.listeners.RuleProfiler;
import ma.s2m.fraudmanager.model.Measurment;

final class StatelessDroolsSession implements DroolsSession {

    private final StatelessKieSession sks;
    private String subject;
    private Long windowSize;

    StatelessDroolsSession(StatelessKieSession sks) { this.sks = sks; }

    @Override public void setGlobal(String name, Object value) { sks.setGlobal(name, value); }

    @Override public void execute(Object... facts) {
        if (facts == null || facts.length == 0) {
            sks.execute(java.util.Collections.emptyList());
        } else {
            List<Measurment> list = new ArrayList<>();
            for (Object f : facts) {
                if (f instanceof Measurment) {
                    list.add((Measurment) f);
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

            KieCommands commands = KieServices.Factory.get().getCommands();
            List<Command<?>> batch = new ArrayList<>();
            batch.add(commands.newInsertElements(list));

            if (Utils.isAgendaGroupByRuleTypeEnabled) {
                batch.add(commands.newAgendaGroupSetFocus(RuleDefinition.RULE_TYPE_ALERT + ":" + this.subject + "->" + formattedDuration));
                batch.add(commands.newAgendaGroupSetFocus(RuleDefinition.RULE_TYPE_COMPUTE + ":" + this.subject + "->" + formattedDuration));
                batch.add(commands.newFireAllRules());
                sks.execute(batch);
            } else {
                batch.add(commands.newAgendaGroupSetFocus(this.subject + "->" + formattedDuration));
                batch.add(commands.newFireAllRules());
                sks.execute(batch);
            }
        }
    }

    @Override
    public void addEventListener(AgendaEventListener eventListener) {
        sks.addEventListener(eventListener);
    }

    @Override
    public void removeEventListener(AgendaEventListener eventListener) {
        sks.removeEventListener(eventListener);
    }

    @Override
    public AgendaEventListener getAgendaEventListener() {
        Collection<AgendaEventListener> listeners = sks.getAgendaEventListeners();
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
    }
}

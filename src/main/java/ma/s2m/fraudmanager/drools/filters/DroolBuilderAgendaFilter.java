package ma.s2m.fraudmanager.drools.filters;

import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.Match;

public class DroolBuilderAgendaFilter implements AgendaFilter {
    private String agendaGroup;

    public DroolBuilderAgendaFilter(String agendaGroup) {
        this.agendaGroup = agendaGroup;
    }

    @Override
    public boolean accept(Match match) {
        Object ruleAgendaGroup = null;
        if (match.getRule().getMetaData() != null) {
            ruleAgendaGroup = match.getRule().getMetaData().get("agenda-group");
        }
        return this.agendaGroup.equals(ruleAgendaGroup);
    }

    public String getAgendaGroup() {
        return agendaGroup;
    }

    public void setAgendaGroup(String agendaGroup) {
        this.agendaGroup = agendaGroup;
    }

}

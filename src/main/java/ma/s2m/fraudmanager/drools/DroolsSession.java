package ma.s2m.fraudmanager.drools;

import org.kie.api.event.rule.AgendaEventListener;

// 2) Abstraction commune à consommer partout
public interface DroolsSession extends AutoCloseable {
    void setGlobal(String name, Object value);
    void execute(Object... facts);        // insert + fireAllRules (stateful) ou execute (stateless)
    default void warmUp() { execute(); }  // compile/charge les règles
    @Override default void close() {}     // no-op par défaut
    default void clean() {} // alias pour AutoCloseable
    void addEventListener(AgendaEventListener eventListener);
    void removeEventListener(AgendaEventListener eventListener);
    AgendaEventListener getAgendaEventListener();
    void dispose();
    default boolean isBroken() { return false; }
}

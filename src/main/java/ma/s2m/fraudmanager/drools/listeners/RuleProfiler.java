package ma.s2m.fraudmanager.drools.listeners;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.kie.api.event.rule.*;
import org.drools.core.definitions.rule.impl.RuleImpl;
import org.kie.api.definition.rule.Rule;
import java.util.function.Supplier;

public final class RuleProfiler extends DefaultAgendaEventListener {

  private static final class Stat { long nanos; long fires; }
  private static final Map<String, Stat> stats = new ConcurrentHashMap<>();

  private final ThreadLocal<Long> t0 = ThreadLocal.withInitial(new Supplier<Long>() {
    @Override
    public Long get() {
      return 0L;
    }
  });
  
  private static String key(Rule r) {
    String g = ((RuleImpl) r).getAgendaGroup();
    return (g == null || g.isEmpty() ? "MAIN" : g) + " :: " + r.getName();
  }
  

  public void reset() { stats.clear(); }
  
  @Override public void beforeMatchFired(BeforeMatchFiredEvent e) { t0.set(System.nanoTime()); }

  @Override public void afterMatchFired(AfterMatchFiredEvent e) {
    long dt = System.nanoTime() - t0.get();
    String k = key(e.getMatch().getRule());
    Stat s = stats.computeIfAbsent(k, kk -> new Stat());
    s.nanos += dt; s.fires++;
  }

  public List<String> reportTop(int top) {
    List<Map.Entry<String, Stat>> L = new ArrayList<>(stats.entrySet());
    L.sort((a,b) -> Long.compare(b.getValue().nanos, a.getValue().nanos));
    List<String> out = new ArrayList<>();
    for (int i=0; i<Math.min(top, L.size()); i++) {
      Map.Entry<String, Stat> e = L.get(i);
      out.add(String.format("%6.2f ms | x%4d | %s",
              e.getValue().nanos/1_000_000.0, e.getValue().fires, e.getKey()));
    }
    return out;
  }
}

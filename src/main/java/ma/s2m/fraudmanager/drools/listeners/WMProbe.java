package ma.s2m.fraudmanager.drools.listeners;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.kie.api.event.rule.*;

public final class WMProbe extends DefaultRuleRuntimeEventListener {
  public final ConcurrentHashMap<String, LongAdder> ins = new ConcurrentHashMap<>();
  public final ConcurrentHashMap<String, LongAdder> upd = new ConcurrentHashMap<>();
  public final ConcurrentHashMap<String, LongAdder> del = new ConcurrentHashMap<>();


  private static String cls(Object o){ return o==null? "null" : o.getClass().getSimpleName(); }

  public void reset() { ins.clear(); upd.clear(); del.clear(); }

  @Override public void objectInserted(ObjectInsertedEvent e) {
    ins.computeIfAbsent(cls(e.getObject()), k->new LongAdder()).increment();
  }
  @Override public void objectUpdated(ObjectUpdatedEvent e) {
    upd.computeIfAbsent(cls(e.getObject()), k->new LongAdder()).increment();
  }
  @Override public void objectDeleted(ObjectDeletedEvent e) {
    del.computeIfAbsent(cls(e.getOldObject()), k->new LongAdder()).increment();
  }
  public String dump(){
    String out = "";
    out += "Inserts : " + ins + "\n";
    out += "Updates : " + upd + "\n";
    out += "Retracts: " + del + "\n";
    return out;
  }
}

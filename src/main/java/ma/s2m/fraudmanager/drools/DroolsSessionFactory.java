package ma.s2m.fraudmanager.drools;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieModule;
import org.kie.api.builder.KieRepository;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;

import ma.s2m.fraudmanager.config.AppConfig;

// 4) Fabrique qui prépare KieContainer + crée la session voulue
public final class DroolsSessionFactory {

    private final KieContainer kieContainer;

    public DroolsSessionFactory(String extendedVersion) throws IOException {
        // ex: extendedVersion = "myrules-1.0.3"
        String ruleset = extendedVersion.split("-")[0];
        String version  = extendedVersion.substring(ruleset.length() + 1);

        Path kjar = java.nio.file.Paths.get(
            AppConfig.ruleDeploymentDir + java.io.File.separator + ruleset + "-" + version + ".jar");

        KieServices ks = KieServices.Factory.get();
        KieRepository repo = ks.getRepository();
        KieModule km = repo.addKieModule(ks.getResources().newFileSystemResource(kjar.toFile()));
        if (km == null) throw new IllegalStateException(
            "JAR not recognized as a kjar (kmodule.xml/pom.properties missing ?)");

        this.kieContainer = ks.newKieContainer(km.getReleaseId());
    }

    public DroolsSession newSession(SessionMode mode, java.util.Map<String, Object> globals) {
        DroolsSession session;
        KieBase kbase;

        try {
            kbase = kieContainer.getKieBase();
        } catch (Exception e) {
            // fallback: prendre le premier KieBase disponible
            String any = kieContainer.getKieBaseNames().iterator().next();
            kbase = kieContainer.getKieBase(any);
        }

        switch (mode) {
            case STATELESS : 
                StatelessKieSession sks = kbase.newStatelessKieSession();
                session = new StatelessDroolsSession(sks);
                break;
    
            case STATEFUL :
                KieSession ks = kbase.newKieSession();
                session = new StatefulDroolsSession(ks);
                break;

            default :
                throw new IllegalArgumentException("Unsupported mode: " + mode);
        }

        if (globals != null) {
            for(Map.Entry <String, Object> entry : globals.entrySet()) {
                String name = entry.getKey();
                Object value = entry.getValue();
                session.setGlobal(name, value);
            }
        }

        session.warmUp(); // charge/compile une première fois
        return session;
    }
}


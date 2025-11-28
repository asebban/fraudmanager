package ma.s2m.fraudmanager.drools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieModule;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Results;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ma.s2m.fraudmanager.config.AppConfig;

// 4) Fabrique qui prépare KieContainer + crée la session voulue
public final class DroolsSessionFactory {

    private final KieContainer kieContainer;
    private static final Logger logger = LoggerFactory.getLogger(DroolsSessionFactory.class);

    private static void validateDrl(KieServices ks) throws IOException {
        Path rulesDir = Paths.get(
                AppConfig.repositoryWorkspaceDirectory + File.separator + "src/main/resources/com/fraudmanager/rules",
                "computes");
        KieFileSystem kfs = ks.newKieFileSystem();

        try (Stream<Path> walk = Files.walk(rulesDir)) {
            walk.filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".drl"))
                    .forEach(p -> {
                        Resource res = ks.getResources().newFileSystemResource(p.toFile());
                        kfs.write("src/main/resources/rules/" + rulesDir.relativize(p), res);
                    });
        }

        KieBuilder kieBuilder = ks.newKieBuilder(kfs).buildAll(); // ou buildAll(ExecutableModelProject.class)
        Results results = kieBuilder.getResults();

        if (results.hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
            StringBuilder sb = new StringBuilder("Error(s) found:\n");
            results.getMessages(org.kie.api.builder.Message.Level.ERROR)
                    .forEach(m -> sb.append(" - ").append(m.getText()).append("\n"));
            logger.error("Error(s) found: {}", sb.toString());
            throw new IllegalStateException(sb.toString());
        }
    }

    public DroolsSessionFactory(String extendedVersion) throws IOException {
        // ex: extendedVersion = "myrules-1.0.3"
        String ruleset = extendedVersion.split("-")[0];
        String version = extendedVersion.substring(ruleset.length() + 1);

        Path kjar = java.nio.file.Paths.get(
                AppConfig.ruleDeploymentDir + java.io.File.separator + ruleset + "-" + version + ".jar");

        if (!kjar.toFile().exists()) {
            throw new IllegalStateException("KJAR file not found: " + kjar.toString());
        }
        logger.info("Loading KJAR from: " + kjar.toString());

        KieServices ks = KieServices.Factory.get();
        validateDrl(ks);

        KieRepository repo = ks.getRepository();
        try {
            KieModule km = repo.addKieModule(ks.getResources().newFileSystemResource(kjar.toFile()));
            if (km == null)
                throw new IllegalStateException("JAR not recognized as a kjar (kmodule.xml/pom.properties missing ?)");
            this.kieContainer = ks.newKieContainer(km.getReleaseId());
        } catch (Exception e) {
            logger.error("Failed to load KJAR: " + kjar.toString(), e);
            throw new IllegalStateException("Error loading KJAR", e);
        }
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
            case STATELESS:
                StatelessKieSession sks = kbase.newStatelessKieSession();
                session = new StatelessDroolsSession(sks);
                break;

            case STATEFUL:
                KieSession ks = kbase.newKieSession();
                session = new StatefulDroolsSession(ks);
                break;

            default:
                throw new IllegalArgumentException("Unsupported mode: " + mode);
        }

        if (globals != null) {
            for (Map.Entry<String, Object> entry : globals.entrySet()) {
                String name = entry.getKey();
                Object value = entry.getValue();
                session.setGlobal(name, value);
            }
        }

        //session.warmUp(); // charge/compile une première fois
        return session;
    }
}

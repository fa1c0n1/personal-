package com.apple.aml.stargate.cli.commands;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.apple.aml.stargate.cli.boot.StargateClient.isBlank;
import static java.nio.charset.StandardCharsets.UTF_8;

@Command(name = "operator", mixinStandardHelpOptions = true, description = "setup/modify stargate operator on the desired kubernetes cluster")
public class OperatorSetup implements Callable<Integer> {
    @Option(names = {"-kc", "--kubernetes-context"}, description = "kubernetes context/cluster name", required = true)
    private String k8sContext;
    @Option(names = {"-kn", "--kubernetes-namespace"}, description = "kubernetes namespace; default value : ${DEFAULT-VALUE}")
    private String k8sNamespace = "default";
    @Option(names = {"-appId", "--idms-appId"}, description = "idms appId; required for create/update actions")
    private int appId;
    @Option(names = {"-appKey", "--idms-appKey"}, description = "idms application id key ( IDMS PROD key only ); required for create/update actions")
    private String appKey;
    @Option(names = {"-appPassword", "--idms-password"}, description = "idms password ( IDMS PROD password only ); required for create/update actions")
    private String appPassword;
    @Option(names = {"-env", "--app-mode", "--app-environment"}, description = "stargate app environment to connect to. valid values : UAT, PROD; default value : ${DEFAULT-VALUE}")
    private String env = "UAT";
    @Option(names = {"-ver", "--sg-ver", "--stargate-version"}, description = "stargate operator version; default value : ${DEFAULT-VALUE}")
    private String version = "latest";
    @Option(names = {"-pvc", "--sg-pvc-claim", "--stargate-pipeline-pvc-claim"}, description = "pvc claim to be used for stargate pipeline")
    private String pvc = "";
    @Option(names = {"-cpu", "--sg-op-cpu", "--stargate-operator-cpu"}, description = "cpu to be allocated for stargate operator container; default value : ${DEFAULT-VALUE}")
    private String cpu = "256m";
    @Option(names = {"-mem", "--sg-op-memory", "--stargate-operator-memory"}, description = "memory to be allocated for stargate operator container; default value : ${DEFAULT-VALUE}")
    private String memory = "500Mi";
    @Parameters(index = "0", description = "action to perform. valid actions : ${COMPLETION-CANDIDATES}")
    private Action action;

    @Override
    public Integer call() throws Exception {
        try {
            Environment environment;
            try {
                environment = Environment.valueOf(env.toUpperCase());
            } catch (Exception e) {
                environment = null;
            }
            if (environment == null) {
                environment = Environment.UAT;
            }
            switch (action) {
                case create:
                case update:
                    return createOrUpdateCluster(environment);
                case delete:
                    return -1;
            }
            return 0;
        } catch (Exception e) {
            System.err.println(String.format("Error: %s: %s", e.getClass().getSimpleName(), e.getMessage()));
            throw e;
        }
    }

    int createOrUpdateCluster(Environment environment) throws Exception {
        if (isBlank(k8sContext)) throw new IllegalArgumentException("kubernetes context is a required argument");
        if (appId <= 0) throw new IllegalArgumentException("appId should be a valid IDMS appId");
        if (isBlank(appKey)) throw new IllegalArgumentException("appKey is a required argument for current action");
        if (isBlank(appPassword)) throw new IllegalArgumentException("appPassword is a required argument for current action");
        String dsKey = Base64.getEncoder().encodeToString(appKey.getBytes(UTF_8));
        String dsPassword = Base64.getEncoder().encodeToString(appPassword.getBytes(UTF_8));
        String yaml = IOUtils.resourceToString("/operator.yml", UTF_8);
        Map<String, String> replacements = Map.of("APP_ID", String.valueOf(appId), "APP_DS_KEY", dsKey, "APP_DS_PASSWORD", dsPassword, "APP_MODE", environment.name(), "APP_PVC_CLAIM", pvc, "STARGATE_VERSION", version, "STARGATE_OPERATOR_CPU", cpu, "STARGATE_OPERATOR_MEMORY", memory);
        for (Map.Entry<String, String> tokenEntry : replacements.entrySet()) {
            yaml = yaml.replaceAll("\\#\\{" + tokenEntry.getKey() + "\\}", tokenEntry.getValue());
        }
        Path tmpPath = Files.createTempFile("stargate-", "-operator.yml");
        Files.writeString(tmpPath, yaml);
        String yamlFileName = tmpPath.toFile().getAbsolutePath();
        System.out.println(String.format("Applying kubectl using file %s on context %s", yamlFileName, k8sContext));
        CommandLine cmdLine = CommandLine.parse(String.format("kubectl apply -f %s --context=%s", yamlFileName, k8sContext));
        DefaultExecutor executor = new DefaultExecutor();
        int exitValue = executor.execute(cmdLine);
        return exitValue;
    }

    enum Action {
        create, update, delete
    }

    enum Environment {
        LOCAL, DEV, QA, UAT, PROD
    }
}

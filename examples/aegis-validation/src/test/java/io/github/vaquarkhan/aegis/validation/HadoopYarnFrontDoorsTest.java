/*
 * Copyright 2026 Viquar Khan and the Aegis MCP Gateway contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.vaquarkhan.aegis.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vaquarkhan.aegis.engine.catalog.SignedServerCatalog;
import io.github.vaquarkhan.aegis.validation.frontdoor.AdopterScenario;
import io.github.vaquarkhan.aegis.validation.frontdoor.FrontDoor;
import io.github.vaquarkhan.aegis.validation.frontdoor.GovernedFlow;
import io.github.vaquarkhan.aegis.validation.frontdoor.Persona;
import io.github.vaquarkhan.aegis.validation.frontdoor.SignedServers;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.util.Records;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * examples/adopters/yarn: the canonical {@code adopters/configurations/yarn/yarn.yaml} governing a
 * REAL Apache Hadoop YARN cluster, through both front doors. Without Knox, Aegis's own server
 * authenticates static bearer tokens. With Knox, a stock Knox 3.0.0 gateway authenticates the same
 * people against its demo LDAP and admits the route; Aegis governs the tool call in the body. The
 * story and the real effect (a RUNNING YARN application reaching KILLED only after an independent
 * approval) are the same.
 */
class HadoopYarnFrontDoorsTest {

    private static MiniYARNCluster yarn;
    private static YarnClient yarnClient;
    private static String rmWebAddress;

    @TempDir Path dir;

    @BeforeAll
    static void startYarn() throws Exception {
        YarnConfiguration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "127.0.0.1:0");
        conf.set(YarnConfiguration.NM_WEBAPP_ADDRESS, "127.0.0.1:0");
        conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        conf.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        conf.setInt(YarnConfiguration.NM_PMEM_MB, 2048);

        yarn = new MiniYARNCluster("aegis-yarn-test", 1, 1, 1);
        yarn.init(conf);
        yarn.start();

        Configuration startedConf = yarn.getConfig();
        rmWebAddress = startedConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS);

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(startedConf);
        yarnClient.start();

        io.github.vaquarkhan.aegis.validation.frontdoor.RunRecord.engineInProcess(
                "Apache Hadoop YARN runs inside the test JVM as a `MiniYARNCluster` (1 ResourceManager, "
                        + "1 NodeManager) with its ResourceManager REST endpoint on `"
                        + rmWebAddress
                        + "`: the same `/ws/v1/cluster` REST API a standalone or HA YARN cluster serves. "
                        + "Per front door, the test submits a real sleeping application via `YarnClient`, "
                        + "which `yarn_kill_app` then transitions to `KILLED`.");
    }

    @AfterAll
    static void stopYarn() throws Exception {
        if (yarnClient != null) {
            yarnClient.close();
        }
        if (yarn != null) {
            yarn.close();
        }
    }

    @ParameterizedTest(name = "yarn through {0}")
    @ValueSource(strings = {FrontDoor.OWN_HOST, FrontDoor.KNOX})
    void governsARealYarnCluster(String door) throws Exception {
        AdopterScenario scenario = scenario();
        try (FrontDoor d = FrontDoor.open(door, scenario, dir)) {
            ApplicationId appId = submitSleepingApp();
            String aid = appId.toString();
            d.note("submitted real YARN application " + aid + "; RM REST http://" + rmWebAddress);

            GovernedFlow.read(d, "yarn_list_apps", Map.of(), aid);
            GovernedFlow.destructive(
                    d,
                    "yarn_kill_app",
                    Map.of("appId", aid),
                    () ->
                            assertEquals(
                                    YarnApplicationState.RUNNING,
                                    yarnClient.getApplicationReport(appId).getYarnApplicationState(),
                                    "denied calls never touched the application"),
                    () -> {
                        waitFor(appId, YarnApplicationState.KILLED);
                        d.note(
                                "YARN reports application "
                                        + aid
                                        + " "
                                        + yarnClient.getApplicationReport(appId).getYarnApplicationState());
                    });
        }
    }

    private static AdopterScenario scenario() throws Exception {
        return AdopterScenario.of(
                "yarn",
                AdopterScenario.canonical("yarn", "yarn.yaml"),
                yaml ->
                        yaml.replace("http://yarn-resourcemanager:8088", "http://" + rmWebAddress)
                                .replace("allowCidrs: [10.0.0.0/8]", "allowCidrs: [127.0.0.0/8]"),
                Map.of(
                        Persona.READER,
                        List.of("yarn-users"),
                        Persona.OPERATOR,
                        List.of("yarn-operators"),
                        Persona.APPROVER,
                        List.of("yarn-approvers")),
                Map.of());
    }

    /**
     * The same real cluster with a signed server catalog: the door lists exactly the reviewed YARN
     * tools and governs a real read; a break-glass order that contains the only server leaves nothing
     * to start, so the gateway refuses to start rather than serve an empty or unreviewed catalog.
     */
    @ParameterizedTest(name = "yarn signed server catalog through {0}")
    @ValueSource(strings = {FrontDoor.OWN_HOST, FrontDoor.KNOX})
    void aSignedServerCatalogGovernsTheRealClusterAndBreakGlassContainsIt(String door)
            throws Exception {
        SignedServers.Signed signed =
                SignedServers.sign(scenario(), "yarn-server-catalog", dir, Map.of());
        try (FrontDoor d = FrontDoor.open(door, signed.scenario(), dir.resolve("signed"))) {
            ApplicationId appId = submitSleepingApp();
            d.note(
                    "server catalog signed for "
                            + signed.pins().size()
                            + " YARN tools; submitted real application "
                            + appId);
            SignedServers.assertOnlySignedToolsAreActive(d, signed);
            GovernedFlow.read(d, "yarn_list_apps", Map.of(), appId.toString());
            yarnClient.killApplication(appId);
        }

        SignedServers.Signed contained =
                SignedServers.sign(
                        scenario(),
                        "yarn-break-glass",
                        dir,
                        Map.of("yarn", SignedServerCatalog.State.QUARANTINED));
        Exception refused =
                assertThrows(
                        Exception.class,
                        () -> FrontDoor.open(door, contained.scenario(), dir.resolve("contained")).close());
        String why = SignedServers.messages(refused);
        assertTrue(
                why.contains(
                        FrontDoor.OWN_HOST.equals(door)
                                ? "no source that the signed server catalog lets start"
                                : "Aegis failed to load inside Knox"),
                why);
    }

    /**
     * The signature binds the reviewed configuration: pointing the source elsewhere stops the
     * gateway.
     */
    @org.junit.jupiter.api.Test
    void editingTheReviewedYarnSourceStopsTheGateway() throws Exception {
        SignedServers.Signed signed =
                SignedServers.sign(scenario(), "yarn-server-catalog-edited", dir, Map.of());
        AdopterScenario s = signed.scenario();
        AdopterScenario moved =
                new AdopterScenario(
                        s.adopter(),
                        s.configYaml().replace("http://" + rmWebAddress, "http://127.0.0.2:8088"),
                        s.groups(),
                        s.env(),
                        s.knoxPlugins(),
                        s.knoxJvmArgs(),
                        s.baseDirFiles());
        Exception refused =
                assertThrows(
                        Exception.class,
                        () -> FrontDoor.open(FrontDoor.OWN_HOST, moved, dir.resolve("edited")).close());
        String why = SignedServers.messages(refused);
        assertTrue(
                why.contains("differs from the configuration reviewed in the signed server catalog"), why);
    }

    private static ApplicationId submitSleepingApp() throws Exception {
        YarnClientApplication app = yarnClient.createApplication();
        ApplicationSubmissionContext ctx = app.getApplicationSubmissionContext();
        ctx.setApplicationName("aegis-governed-sleep");
        ctx.setApplicationType("YARN");
        ctx.setQueue("default");

        ContainerLaunchContext amSpec = Records.newRecord(ContainerLaunchContext.class);
        amSpec.setCommands(List.of("sleep 300"));
        ctx.setAMContainerSpec(amSpec);
        ctx.setResource(Resource.newInstance(128, 1));

        ApplicationId appId = ctx.getApplicationId();
        yarnClient.submitApplication(ctx);
        waitFor(appId, YarnApplicationState.RUNNING);
        return appId;
    }

    private static void waitFor(ApplicationId id, YarnApplicationState target) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            ApplicationReport report = yarnClient.getApplicationReport(id);
            if (report.getYarnApplicationState() == target) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "application did not reach "
                        + target
                        + " (now "
                        + yarnClient.getApplicationReport(id).getYarnApplicationState()
                        + ")");
    }
}
/*
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
package io.prestosql.server;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.bdp.KSGatewayListener;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.event.client.HttpEventModule;
import io.airlift.event.client.JsonEventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.units.Duration;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.eventlistener.EventListenerModule;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.warnings.WarningCollectorModule;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.StaticCatalogStore;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AccessControlModule;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.server.security.ServerSecurityModule;
import io.prestosql.sql.parser.SqlParserOptions;
import org.weakref.jmx.guice.MBeanModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.prestosql.server.PrestoSystemRequirements.verifyJvmRequirements;
import static io.prestosql.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PrestoServer
        implements Runnable
{
    private static final int DEFAULT_RETRY_ATTEMPTS = 5;
    private static final Duration DEFAULT_SLEEP_TIME = Duration.valueOf("1s");
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.valueOf("10s");
    private static final double DEFAULT_SCALE_FACTOR = 2.0;

    public static void main(String[] args)
    {
        new PrestoServer().run();
    }

    private final SqlParserOptions sqlParserOptions;

    public PrestoServer()
    {
        this(new SqlParserOptions());
    }

    public PrestoServer(SqlParserOptions sqlParserOptions)
    {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
    }

    @Override
    public void run()
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        Logger log = Logger.get(PrestoServer.class);

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new NodeModule(),
                new DiscoveryModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new PrefixObjectNameGeneratorModule("io.prestosql"),
                new JmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new JsonEventModule(),
                new HttpEventModule(),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new EventListenerModule(),
                new ServerMainModule(sqlParserOptions),
                new GracefulShutdownModule(),
                new WarningCollectorModule());

        modules.addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.strictConfig().initialize();

            logLocation(log, "Working directory", Paths.get("."));
            logLocation(log, "Etc directory", Paths.get("etc"));

            injector.getInstance(PluginManager.class).loadPlugins();

            injector.getInstance(StaticCatalogStore.class).loadCatalogs();

            // TODO: remove this huge hack
            updateConnectorIds(
                    injector.getInstance(Announcer.class),
                    injector.getInstance(CatalogManager.class),
                    injector.getInstance(ServerConfig.class),
                    injector.getInstance(NodeSchedulerConfig.class));

            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(EventListenerManager.class).loadConfiguredEventListener();

            injector.getInstance(Announcer.class).start();
            ServerInfoResource serverInfoResource = injector.getInstance(ServerInfoResource.class);
            serverInfoResource.startupComplete();

            log.info("======== SERVER STARTED ========");

            QueryManagerConfig queryManagerConfig = injector.getInstance(QueryManagerConfig.class);

            // Register the Presto server with keystone service to provide lineage service.
            // Even if registration fails, Presto server continues to work correctly.
            LineageStats lineageStats = injector.getInstance(LineageStats.class);
            registerWithLineageService(lineageStats, serverInfoResource, queryManagerConfig);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    public void registerWithLineageService(LineageStats lineageStats, ServerInfoResource serverInfoResource, QueryManagerConfig queryManagerConfig)
    {
        long startTime = System.nanoTime();
        int attempt = 0;
        while (true) {
            attempt++;

            try {
                KSGatewayListener.initialize("presto", "presto", serverInfoResource.getInfo().getNodeVersion().getVersion(),
                            queryManagerConfig.getLineageLoggingHost(), queryManagerConfig.getLineageLoggingPort());
                lineageStats.newLineageRetry(0);
                return;
            }
            catch (Exception e) {
                if (attempt >= DEFAULT_RETRY_ATTEMPTS || Duration.nanosSince(startTime).compareTo(DEFAULT_MAX_RETRY_TIME) >= 0) {
                    lineageStats.newLineageRetry(attempt);
                    return;
                }

                int delayInMs = (int) Math.min(DEFAULT_SLEEP_TIME.toMillis() * Math.pow(DEFAULT_SCALE_FACTOR, attempt - 1), DEFAULT_SLEEP_TIME.toMillis());
                int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayInMs * 0.1)));
                try {
                    MILLISECONDS.sleep(delayInMs + jitter);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
    }

    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }

    private static void updateConnectorIds(Announcer announcer, CatalogManager metadata, ServerConfig serverConfig, NodeSchedulerConfig schedulerConfig)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        Set<String> connectorIds = new LinkedHashSet<>();

        // automatically build connectorIds if not configured
        List<Catalog> catalogs = metadata.getCatalogs();
        // if this is a dedicated coordinator, only add jmx
        if (serverConfig.isCoordinator() && !schedulerConfig.isIncludeCoordinator()) {
            catalogs.stream()
                    .map(Catalog::getConnectorCatalogName)
                    .filter(connectorId -> connectorId.getCatalogName().equals("jmx"))
                    .map(Object::toString)
                    .forEach(connectorIds::add);
        }
        else {
            catalogs.stream()
                    .map(Catalog::getConnectorCatalogName)
                    .map(Object::toString)
                    .forEach(connectorIds::add);
        }

        // build announcement with updated sources
        ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        builder.addProperties(announcement.getProperties());
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

    private static void logLocation(Logger log, String name, Path path)
    {
        if (!Files.exists(path, NOFOLLOW_LINKS)) {
            log.info("%s: [does not exist]", name);
            return;
        }
        try {
            path = path.toAbsolutePath().toRealPath();
        }
        catch (IOException e) {
            log.info("%s: [not accessible]", name);
            return;
        }
        log.info("%s: %s", name, path);
    }
}

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
package io.prestosql.plugin.druid;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import org.apache.calcite.avatica.remote.Driver;

import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DruidJdbcClientModule extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).to(DruidJdbcClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DruidConfig.class);
        ensureCatalogIsEmpty(buildConfigObject(BaseJdbcConfig.class).getConnectionUrl());
    }

    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        /*try {
            Driver driver = new Driver();
            //Properties urlProperties = driver.acceptsURL(
            //checkArgument(urlProperties != null, "Invalid JDBC URL for MySQL connector");
            //checkArgument(driver.database(urlProperties) == null, "Database (catalog) must not be specified in JDBC URL for MySQL connector");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }*/
    }

//    //@ForBaseJdbc
    @Provides
    @Singleton
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, DruidConfig druidConfig)
    {
        Properties connectionProperties = new Properties();
        return new DriverConnectionFactory(
                new Driver(),
                //"config.getConnectionUrl()",
                "jdbc:avatica:remote:url=http://bdp_druid-test-broker-experimental.cluster.us-east-1.prod.cloud.netflix.net:7103/druid/v2/sql/avatica/",
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }
}

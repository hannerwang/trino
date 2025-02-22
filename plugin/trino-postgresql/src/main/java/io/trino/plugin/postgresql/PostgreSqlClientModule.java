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
package io.trino.plugin.postgresql;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteQueryCancellationModule;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import org.postgresql.Driver;

import java.util.Properties;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static org.postgresql.PGProperty.REWRITE_BATCHED_INSERTS;

public class PostgreSqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(PostgreSqlClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PostgreSqlConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindSessionPropertiesProvider(binder, PostgreSqlSessionProperties.class);
        newOptionalBinder(binder, QueryBuilder.class).setBinding().to(CollationAwareQueryBuilder.class).in(Scopes.SINGLETON);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new RemoteQueryCancellationModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        Properties connectionProperties = new Properties();
        connectionProperties.put(REWRITE_BATCHED_INSERTS.getName(), "true");
        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), connectionProperties, credentialProvider);
    }
}

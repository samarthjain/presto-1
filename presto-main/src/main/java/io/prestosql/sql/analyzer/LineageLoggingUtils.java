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
package io.prestosql.sql.analyzer;

import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.metadata.QualifiedObjectName;

import java.util.HashMap;
import java.util.Map;

public final class LineageLoggingUtils
{
    private LineageLoggingUtils() {}

    public static Map<String, String> getSessionProperties(Session session)
    {
        Map<String, String> props = new HashMap<>();
        props.putAll(session.getSystemProperties());

        // Lineage Logging library requires the key to be genie_id and genie_name instead of
        // genie_job_id and genie_job_name that is in the SessionSystemProperties.
        props.remove("genie_job_id");
        props.remove("genie_job_name");
        props.put("genie_id", session.getSystemProperty("genie_job_id", String.class));
        props.put("genie_name", session.getSystemProperty("genie_job_name", String.class));

        props.put("user", session.getUser());
        props.put("queryId", session.getQueryId().toString());

        session.getIdentity().getSessionPropertiesByCatalog().forEach((k, v) -> props.putAll(v));
        return props;
    }

    // This method returns the object name where the catalog name is a metacat catalog name
    public static String objectNameToString(Session session, QualifiedObjectName objectName)
    {
        String catalogName = objectName.getCatalogName();
        String metacatCatalogName;
        if (SystemSessionProperties.getMetacatCatalogMapping(session).containsKey(catalogName)) {
            metacatCatalogName = SystemSessionProperties.getMetacatCatalogMapping(session).get(catalogName);
        }
        else {
            metacatCatalogName = catalogName;
        }
        return metacatCatalogName + "." + objectName.getSchemaName() + "." + objectName.getObjectName();
    }
}

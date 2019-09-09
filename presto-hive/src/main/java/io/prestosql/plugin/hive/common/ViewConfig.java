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
package io.prestosql.plugin.hive.common;

import io.airlift.configuration.Config;

public class ViewConfig
{
    private String metastoreRestEndpoint;
    private String metacatCatalogName;
    private String metastoreWarehouseDir;

    public String getMetastoreRestEndpoint()
    {
        return metastoreRestEndpoint;
    }

    @Config("view.metacat-rest-endpoint")
    public void setMetastoreRestEndpoint(String metastoreRestEndpoint)
    {
        this.metastoreRestEndpoint = metastoreRestEndpoint;
    }

    public String getMetacatCatalogName()
    {
        return metacatCatalogName;
    }

    @Config("view.metacat-catalog-name")
    public void setMetacatCatalogName(String metacatCatalogName)
    {
        this.metacatCatalogName = metacatCatalogName;
    }

    @Config("view.metastore-warehouse-dir")
    public void setMetastoreWarehouseDir(String metastoreWarehouseDir)
    {
        this.metastoreWarehouseDir = metastoreWarehouseDir;
    }

    public String getMetastoreWarehouseDir()
    {
        return metastoreWarehouseDir;
    }
}

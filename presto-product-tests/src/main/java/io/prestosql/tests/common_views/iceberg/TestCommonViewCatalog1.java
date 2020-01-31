package io.prestosql.tests.common_views.iceberg;

import io.airlift.log.Logger;
import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.COMMON_VIEW;

public class TestCommonViewCatalog1
        extends ProductTest
{
    /*
     * PLEASE READ: This is Part #1 of a three part test.
     * Part #1: Simulate production environment in development environment (by leaving the settings be).
     *          The test creates some common views in various catalogs.
     * Part #2: Simulate test environment in development environment by copying 'testhive/testiceberg.properties'
     *          to 'hive/iceberg.properties' and changing 'metacat_catalog_mapping for hive from 'prodhive' to
     *          'testhive' and 'iceberg' to 'testhive' and restarting the presto cluster.
     *          The test ensures that views created in Part #1 can be accessed when expected.
     * Part #3: Simulate production environment in development environment by reversing set up in Part #2.
     *          (Copy 'prodhive/prodiceberg.properties' to 'hive/iceberg.properties' and changing the
     *          'metacat_catalog_mapping for hive from 'testhive' to 'prodhive' and 'iceberg' to 'prodhive'
     *          and restarting the presto cluster.
     *          The test ensures that the created views are dropped correctly.
     */
    private static final String BASE1 = "base_tab_ice";
    private static final String BASE2 = "testhive_base_tab_ice";

    @BeforeTestWithContext
    public void createObjects()
    {
        query("create schema if not exists testhive.cat_test");
        query("use testiceberg.cat_test");
        query("create table if not exists testhive.cat_test." + BASE2 + "(c1 int, c2 int)");

        query("create schema if not exists iceberg.cat_test");
        query("use iceberg.cat_test");
        query("create table if not exists " + BASE1 + "(c1 int, c2 int)");

        query("create or replace view prod_view_ice as select * from " + BASE1);

        query("create or replace view prod_view_ice2 as select * from testhive.cat_test." + BASE2);

        query("create or replace view prod_view_ice3 as select * from hive.cat_test." + BASE1);

        query("create or replace view hive.cat_test.prod_view_ice4 as select * from hive.cat_test." + BASE1);
    }

    @AfterTestWithContext
    public void dropObjects()
    {
    }

    @Test(groups = COMMON_VIEW)
    public void testCommonViewCatalog1()
    {
        query("use iceberg.cat_test");
        query("select * from prod_view_ice");
        query("select * from prod_view_ice2");
        query("select * from prod_view_ice3");
        query("select * from prod_view_ice4");
        query("select * from prodhive.cat_test.prod_view_ice");
        query("select * from prodhive.cat_test.prod_view_ice2");
        query("select * from prodhive.cat_test.prod_view_ice3");
        query("select * from prodhive.cat_test.prod_view_ice4");

        query("use prodiceberg.cat_test");

        query("select * from prod_view_ice");
        query("select * from prod_view_ice2");
        query("select * from prod_view_ice3");
        query("select * from prod_view_ice4");

        query("use testiceberg.cat_test");

        // does not work, it is expected
        try {
            query("select * from prod_view_ice");
        } catch (Exception e) {
            Logger.get(getClass()).warn(e, "Catalog resolution is expected to fail.");
        }
        try {
            query("select * from prod_view_ice2");
        } catch (Exception e) {
            Logger.get(getClass()).warn(e, "Catalog resolution is expected to fail.");
        }
        try {
            query("select * from prod_view_ice3");
        } catch (Exception e) {
            Logger.get(getClass()).warn(e, "Catalog resolution is expected to fail.");
        }
        try {
            query("select * from prod_view_ice4");
        } catch (Exception e) {
            Logger.get(getClass()).warn(e, "Catalog resolution is expected to fail.");
        }

        query("select * from prodhive.cat_test.prod_view_ice");
        query("select * from prodhive.cat_test.prod_view_ice2");
        query("select * from prodhive.cat_test.prod_view_ice3");
        query("select * from prodhive.cat_test.prod_view_ice4");
    }
}

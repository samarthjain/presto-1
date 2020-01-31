package io.prestosql.tests.common_views.iceberg;

import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.COMMON_VIEW;

public class TestCommonViewCatalog3
        extends ProductTest
{
    /*
     * PLEASE READ: This is Part #3 of a three part test. Please perform set-up outlined in Part #3 of test
     * before running the test.
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

    @BeforeTestWithContext
    public void createObjects()
    {
    }

    @AfterTestWithContext
    public void dropObjects()
    {
    }

    @Test(groups = COMMON_VIEW)
    public void testCommonViewCatalog3()
    {
        query ("use iceberg.cat_test");
        query ("drop view prod_view_ice2");
        query("drop view prodhive.cat_test.prod_view_ice3");
        query ("use prodiceberg.cat_test");
        query ("drop view prod_view_ice4");
    }
}

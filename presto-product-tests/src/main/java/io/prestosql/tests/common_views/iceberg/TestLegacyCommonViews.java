package io.prestosql.tests.common_views.iceberg;

import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.COMMON_VIEW;
import static java.lang.String.format;

/**
 * These tests simply query the common and legacy views created from Spark2.1.1, Spark2.3.2 and Presto.
 * The expectation is that those views will always exist. If the views are dropped/migrated, ok for this
 * test suite to go away.
 */
public class TestLegacyCommonViews
        extends ProductTest
{
    @BeforeTestWithContext
    public void createObjects()
    {
    }

    @AfterTestWithContext
    public void dropObjects()
    {
    }

    @Test(groups = COMMON_VIEW)
    public void testLegacyViews()
    {
        // Test common presto view
        String querySql = format("" + "select * from prodhive.common_view.simple_common_view order by id, value");
        assertThat(query(querySql)).containsExactly(
                        row(1, "one", 20191121),
                        row(1, "uno", 20191121),
                        row(2, "don", 20191120),
                        row(2, "dos", 20191120),
                        row(2, "two", 20191120));

        querySql = format("" + "select * from prodhive.common_view.legacy_view_spark21");
        assertThat(query(querySql)).containsExactly(
                row("def", "bbb"));

        querySql = format("" + "select * from prodhive.common_view.legacy_view_spark23");
        assertThat(query(querySql)).containsExactly(
                row("def", "bbb"));

        querySql = format("" + "select * from prodhive.common_view.legacy_view_presto");
        assertThat(query(querySql)).containsExactly(
                row("def", "bbb"));

        querySql = format("" + "select * from prodhive.common_view.common_view_spark23");
        assertThat(query(querySql)).containsExactly(
                row("def", "bbb"));

        querySql = format("" + "select * from prodhive.common_view.legacy_ice_spark_view_211 order by c1");
        assertThat(query(querySql)).containsExactly(
                row(1, "one"),
                row(2, "two"));

        querySql = format("" + "select * from prodhive.common_view.legacy_ice_spark_view_232 order by c1");
        assertThat(query(querySql)).containsExactly(
                row(1, "one"),
                row(2, "two"));

        querySql = format("" + "select * from prodhive.common_view.legacy_ice_presto_view order by c1");
        assertThat(query(querySql)).containsExactly(
                row(1, "one"),
                row(2, "two"));

        querySql = format("" + "select * from hive.common_view.legacy_ice_spark_view_211 order by c1");
        assertThat(query(querySql)).containsExactly(
                row(1, "one"),
                row(2, "two"));

        querySql = format("" + "select * from iceberg.common_view.legacy_ice_spark_view_232 order by c1");
        assertThat(query(querySql)).containsExactly(
                row(1, "one"),
                row(2, "two"));

        querySql = format("" + "select * from prodiceberg.common_view.legacy_ice_presto_view order by c1");
        assertThat(query(querySql)).containsExactly(
                row(1, "one"),
                row(2, "two"));
    }
}

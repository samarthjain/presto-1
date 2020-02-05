package io.prestosql.tests.common_views.iceberg;

import io.airlift.log.Logger;
import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryExecutionException;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.COMMON_VIEW;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestIcebergCommonViews
        extends ProductTest
{
    private static final String ICEBERG_TABLE_NAME = "testiceberg.ice_common_view.ice_tab" + System.currentTimeMillis();
    private static final String ICEBERG_TABLE_SCHEMA = "testiceberg.ice_common_view";
    private static final String ICEBERG_VIEW_SIMPLE = "testiceberg.ice_common_view.ice_simple";
    private static final String PRESTO_VIEW_SIMPLE = "testiceberg.ice_common_view.ice_simple_native";

    @BeforeTestWithContext
    public void createObjects()
    {
        // Drop Iceberg tables, views and schema
        try {
            query("DROP TABLE IF EXISTS " + ICEBERG_TABLE_NAME);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
        try {
            query("DROP VIEW IF EXISTS " + ICEBERG_VIEW_SIMPLE);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop view");
        }
        try {
            query("DROP VIEW IF EXISTS " + PRESTO_VIEW_SIMPLE);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop view");
        }
        try {
            query("DROP SCHEMA IF EXISTS " + ICEBERG_TABLE_SCHEMA);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop schema");
        }

        // Create iceberg schema, table and insert data
        try {
            query("CREATE SCHEMA IF NOT EXISTS " + ICEBERG_TABLE_SCHEMA);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to create schema");
        }
        try {
            query("CREATE TABLE IF NOT EXISTS " + ICEBERG_TABLE_NAME +
                    "(c1 int, c2 varchar)");
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to create table");
        }
        try {
            query("INSERT INTO " + ICEBERG_TABLE_NAME +
                    " VALUES (1, 'one'), (2, 'two'), (2, 'two'), (2, 'dos')");
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to insert data into the table.");
        }
    }

    @AfterTestWithContext
    public void dropObjects()
    {
        // Drop Iceberg tables, views and schema
        try {
            query("DROP TABLE IF EXISTS " + ICEBERG_TABLE_NAME);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
        try {
            query("DROP VIEW IF EXISTS " + ICEBERG_VIEW_SIMPLE);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop view");
        }
        try {
            query("DROP VIEW IF EXISTS " + PRESTO_VIEW_SIMPLE);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop view");
        }
        try {
            query("DROP SCHEMA IF EXISTS " + ICEBERG_TABLE_SCHEMA);
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop schema");
        }
    }

    @Test(groups = COMMON_VIEW)
    public void testCreateReplaceIcebergViews()
    {
        // Test common view creation
        String createViewSql = format("" +
                        "CREATE VIEW %s AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  %s",
                ICEBERG_VIEW_SIMPLE, ICEBERG_TABLE_NAME);

        query(createViewSql);
        QueryResult actualResult = query("SHOW CREATE VIEW " + ICEBERG_VIEW_SIMPLE);
        assertEquals(actualResult.row(0).get(0), createViewSql);

        // Check the schema of the view
        assertThat(query("SHOW COLUMNS FROM " + ICEBERG_VIEW_SIMPLE).project(1, 2)).containsExactly(
                row("c1", "integer"),
                row("c2", "varchar"));

        assertThat(query("SELECT * FROM " + ICEBERG_VIEW_SIMPLE + " ORDER BY c1, c2"))
                .containsExactly(
                        row(1, "one"),
                        row(2, "dos"),
                        row(2, "two"),
                        row(2, "two"));

        // Try and create the same view without the 'replace' clause. Expect an exception that view exists
        try {
            query(createViewSql);
        } catch (QueryExecutionException e) {
            Logger.get(getClass()).warn(e, "View exists exception as expected.");
        }

        // Try to create a view with a different definition with 'if not exists' clause. The clause is not supported.
        String createViewSql2 = format("" +
                        "CREATE VIEW IF NOT EXISTS %s AS\n" +
                        "SELECT c1, c2\n" +
                        "FROM\n" +
                        "  %s",
                ICEBERG_VIEW_SIMPLE, ICEBERG_TABLE_NAME);
        try {
            query(createViewSql2);
        } catch (QueryExecutionException e) {
            Logger.get(getClass()).warn(e, "'if not exists' clause is not supported. Exception as expected.");
        }

        // Replace the view
        String createViewSql3 = format("" +
                        "CREATE OR REPLACE VIEW %s AS\n" +
                        "SELECT\n" +
                        "  c1\n" +
                        ", c2\n" +
                        "FROM\n" +
                        "  %s",
                ICEBERG_VIEW_SIMPLE, ICEBERG_TABLE_NAME);
        query(createViewSql3);
        actualResult = query("SHOW CREATE VIEW " + ICEBERG_VIEW_SIMPLE);
        String expectedSQL = format("" +
                        "CREATE VIEW %s AS\n" +
                        "SELECT\n" +
                        "  c1\n" +
                        ", c2\n" +
                        "FROM\n" +
                        "  %s",
                ICEBERG_VIEW_SIMPLE, ICEBERG_TABLE_NAME);
        assertEquals(actualResult.row(0).get(0), expectedSQL);

        // Check the schema of the view
        assertThat(query("SHOW COLUMNS FROM " + ICEBERG_VIEW_SIMPLE).project(1, 2)).containsExactly(
                row("c1", "integer"),
                row("c2", "varchar"));

        assertThat(query("SELECT * FROM " + ICEBERG_VIEW_SIMPLE + " ORDER BY c1, c2"))
                .containsExactly(
                        row(1, "one"),
                        row(2, "dos"),
                        row(2, "two"),
                        row(2, "two"));

        // Replace the view once again
        String createViewSql4 = format("" +
                        "CREATE OR REPLACE VIEW %s AS\n" +
                        "SELECT\n" +
                        "  sum(1) as my_sum\n" +
                        ", c2\n" +
                        "FROM\n" +
                        "  %s\n" +
                        "GROUP BY c2",
                ICEBERG_VIEW_SIMPLE, ICEBERG_TABLE_NAME);
        query(createViewSql4);
        actualResult = query("SHOW CREATE VIEW " + ICEBERG_VIEW_SIMPLE);
        expectedSQL = format("" +
                        "CREATE VIEW %s AS\n" +
                        "SELECT\n" +
                        "  sum(1) my_sum\n" +
                        ", c2\n" +
                        "FROM\n" +
                        "  %s\n" +
                        "GROUP BY c2",
                ICEBERG_VIEW_SIMPLE, ICEBERG_TABLE_NAME);
        assertEquals(actualResult.row(0).get(0), expectedSQL);

        // Check the schema of the view
        assertThat(query("SHOW COLUMNS FROM " + ICEBERG_VIEW_SIMPLE).project(1, 2)).containsExactly(
                row("my_sum", "bigint"),
                row("c2", "varchar"));

        assertThat(query("DESCRIBE " + ICEBERG_VIEW_SIMPLE).project(1, 2)).containsExactly(
                row("my_sum", "bigint"),
                row("c2", "varchar"));

        assertThat(query("SELECT * FROM " + ICEBERG_VIEW_SIMPLE + " ORDER BY 1,2"))
                .containsExactly(
                        row(1, "dos"),
                        row(1, "one"),
                        row(2, "two"));

        // Disable common view and check if common view can be read/replaced
        query("set session testiceberg.common_view_support=false");
        try {
            query("SELECT * FROM " + ICEBERG_VIEW_SIMPLE);
        } catch (QueryExecutionException e) {
            Logger.get(getClass()).warn(e, "Common view is disabled. Exception as expected.");
        }

        try {
            query(createViewSql4);
        } catch (QueryExecutionException e) {
            Logger.get(getClass()).warn(e, "Common view is disabled. Exception as expected.");
        }

        // Try creating a presto native view
        String createViewSql5 = format("" +
                        "CREATE VIEW %s AS\n" +
                        "SELECT\n" +
                        "  sum(1) my_sum\n" +
                        ", c2\n" +
                        "FROM\n" +
                        "  %s\n" +
                        "GROUP BY c2",
                PRESTO_VIEW_SIMPLE, ICEBERG_TABLE_NAME);
        query(createViewSql5);
        actualResult = query("SHOW CREATE VIEW " + PRESTO_VIEW_SIMPLE);
        assertEquals(actualResult.row(0).get(0), createViewSql5);
        assertThat(query("SELECT * FROM " + PRESTO_VIEW_SIMPLE + " ORDER BY 1,2"))
                .containsExactly(
                        row(1, "dos"),
                        row(1, "one"),
                        row(2, "two"));
    }
}

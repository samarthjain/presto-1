package io.prestosql.tests.hive;

import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.COMMON_VIEW;

public class TestHiveIdentifierName
        extends ProductTest
{
    private static final String BASE_TABLE_SCHEMA = "testhive.common_view_identifier";
    private static final String SUFFIX = "_" + System.currentTimeMillis() + " ";

    @BeforeTestWithContext
    public void createObjects()
    {

        query("DROP TABLE IF EXISTS " + BASE_TABLE_SCHEMA + ".t13" + SUFFIX);

        query("DROP TABLE IF EXISTS " + BASE_TABLE_SCHEMA + ".tab23_45" + SUFFIX);

        query("DROP TABLE IF EXISTS " + BASE_TABLE_SCHEMA + ".\"Tab789" + SUFFIX + "\"");

        // Create hive schema, table and insert data
        query("CREATE SCHEMA IF NOT EXISTS " + BASE_TABLE_SCHEMA);

        query("CREATE TABLE IF NOT EXISTS " + BASE_TABLE_SCHEMA + ".t13" + SUFFIX +
                "(\"123abc\" int)");

        query("CREATE TABLE IF NOT EXISTS " + BASE_TABLE_SCHEMA + ".tab23_45" + SUFFIX +
                "(\"123 abc\" int)");

        query("CREATE TABLE IF NOT EXISTS " + BASE_TABLE_SCHEMA + ".\"Tab789" + SUFFIX + "\"" +
                "(\"$1 col\" int, \"$2 # col\" varchar, \"col with \"\" in name\" varchar)");

        query("INSERT INTO " + BASE_TABLE_SCHEMA + ".t13" + SUFFIX +
                "VALUES (1), (2)");

        query("INSERT INTO " + BASE_TABLE_SCHEMA + ".tab23_45" + SUFFIX +
                "VALUES (1), (2)");

        query("INSERT INTO " + BASE_TABLE_SCHEMA + ".tab789" + SUFFIX +
                "VALUES (1, 'abc', 'pqr'), (2, 'defg', 'xyzw')");
    }

    @AfterTestWithContext
    public void dropObjects()
    {

        query("DROP TABLE IF EXISTS " + BASE_TABLE_SCHEMA + ".t13" + SUFFIX);

        query("DROP TABLE IF EXISTS " + BASE_TABLE_SCHEMA + ".tab23_45" + SUFFIX);

        query("DROP TABLE IF EXISTS " + BASE_TABLE_SCHEMA + ".\"Tab789" + SUFFIX + "\"");
    }

    @Test(groups = COMMON_VIEW)
    public void testHiveIdentifierName()
    {

        query("select sum(\"123abc\") from " + BASE_TABLE_SCHEMA + ".t13" + SUFFIX);

        query("select sum(\"123 abc\") from " + BASE_TABLE_SCHEMA + ".tab23_45" + SUFFIX);

        query("select * from " + BASE_TABLE_SCHEMA + ".Tab789" + SUFFIX);

        query("select concat(\"$2 # col\", \"col with \"\" in name\") from " + BASE_TABLE_SCHEMA + ".tab789" + SUFFIX);

        query("select length(\"col with \"\" in name\"), length(\"$2 # col\"), sum(\"$1 col\")" +
                "from " + BASE_TABLE_SCHEMA + ".tab789" + SUFFIX + " where \"$1 col\" > 0 group by \"col with \"\" in name\", 2 order by length(\"$2 # col\")");
    }
}

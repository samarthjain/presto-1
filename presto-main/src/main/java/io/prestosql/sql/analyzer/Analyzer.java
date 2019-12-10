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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.netflix.bdp.Events;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.rewrite.StatementRewrite;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.prestosql.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class Analyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;
    private final List<Expression> parameters;
    private final WarningCollector warningCollector;
    private final String originalQueryText;

    public Analyzer(Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Optional<QueryExplainer> queryExplainer,
            List<Expression> parameters,
            WarningCollector warningCollector,
            String originalQueryText)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "query explainer is null");
        this.parameters = parameters;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.originalQueryText = originalQueryText;
    }

    public Analysis analyze(Statement statement)
    {
        return analyze(statement, false);
    }

    public Analysis analyze(Statement statement, boolean isDescribe)
    {
        Statement rewrittenStatement = StatementRewrite.rewrite(session, metadata, sqlParser, queryExplainer, statement, parameters, accessControl, warningCollector);
        Analysis analysis = new Analysis(rewrittenStatement, parameters, isDescribe, originalQueryText);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);
        analyzer.analyze(rewrittenStatement, Optional.empty());

        List<String> viewNames = analysis.getViews();
        // check column access permissions for each table
        Map<String, String> props = LineageLoggingUtils.getSessionProperties(session);
        props.put("query", analysis.getOriginalQueryText());

        final Map<Analysis.AccessControlInfo, Map<QualifiedObjectName, Set<String>>> tableColumnReferences = analysis.getTableColumnReferences();
        for (Map.Entry<Analysis.AccessControlInfo, Map<QualifiedObjectName, Set<String>>> entry : tableColumnReferences.entrySet()) {
            final Map<QualifiedObjectName, Set<String>> value = entry.getValue();
            for (Map.Entry<QualifiedObjectName, Set<String>> tableColumns : value.entrySet()) {
                final Analysis.AccessControlInfo accessControlInfo = entry.getKey();
                final QualifiedObjectName qualifiedObjectName = tableColumns.getKey();
                final Set<String> columnSet = tableColumns.getValue();
                accessControlInfo.getAccessControl().checkCanSelectFromColumns(
                        session.getRequiredTransactionId(),
                        accessControlInfo.getIdentity(),
                        qualifiedObjectName, columnSet);

                if (!qualifiedObjectName.getSchemaName().equalsIgnoreCase("information_schema")) {
                    if (viewNames.contains(qualifiedObjectName.toString())) {
                        Events.sendLoadView(LineageLoggingUtils.objectNameToString(session, qualifiedObjectName),
                                new ArrayList<>(columnSet), props);
                    }
                    else {
                        // Currently filters are not logged
                        Events.sendScan(LineageLoggingUtils.objectNameToString(session, qualifiedObjectName), "",
                                new ArrayList<>(columnSet), props);
                    }
                }
            }
        }
        return analysis;
    }

    static void verifyNoAggregateWindowOrGroupingFunctions(Metadata metadata, Expression predicate, String clause)
    {
        List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), metadata);

        List<FunctionCall> windowExpressions = extractWindowFunctions(ImmutableList.of(predicate));

        List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

        List<Expression> found = ImmutableList.copyOf(Iterables.concat(
                aggregates,
                windowExpressions,
                groupingOperations));

        if (!found.isEmpty()) {
            throw semanticException(EXPRESSION_NOT_SCALAR, predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
        }
    }
}

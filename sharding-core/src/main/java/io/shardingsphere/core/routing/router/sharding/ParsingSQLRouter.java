/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
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
 * </p>
 */

package io.shardingsphere.core.routing.router.sharding;

import com.google.common.base.Optional;
import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.metadata.ShardingMetaData;
import io.shardingsphere.core.optimizer.OptimizeEngineFactory;
import io.shardingsphere.core.optimizer.condition.ShardingConditions;
import io.shardingsphere.core.parsing.SQLParsingEngine;
import io.shardingsphere.core.parsing.parser.context.condition.Column;
import io.shardingsphere.core.parsing.parser.context.condition.GeneratedKeyCondition;
import io.shardingsphere.core.parsing.parser.dialect.mysql.statement.ShowDatabasesStatement;
import io.shardingsphere.core.parsing.parser.dialect.mysql.statement.ShowTablesStatement;
import io.shardingsphere.core.parsing.parser.dialect.mysql.statement.UseStatement;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import io.shardingsphere.core.parsing.parser.sql.dal.DALStatement;
import io.shardingsphere.core.parsing.parser.sql.ddl.DDLStatement;
import io.shardingsphere.core.parsing.parser.sql.dml.insert.InsertStatement;
import io.shardingsphere.core.parsing.parser.sql.dql.select.SelectStatement;
import io.shardingsphere.core.rewrite.SQLBuilder;
import io.shardingsphere.core.rewrite.SQLRewriteEngine;
import io.shardingsphere.core.routing.SQLExecutionUnit;
import io.shardingsphere.core.routing.SQLRouteResult;
import io.shardingsphere.core.routing.type.RoutingEngine;
import io.shardingsphere.core.routing.type.RoutingResult;
import io.shardingsphere.core.routing.type.TableUnit;
import io.shardingsphere.core.routing.type.broadcast.DatabaseBroadcastRoutingEngine;
import io.shardingsphere.core.routing.type.broadcast.TableBroadcastRoutingEngine;
import io.shardingsphere.core.routing.type.complex.ComplexRoutingEngine;
import io.shardingsphere.core.routing.type.ignore.IgnoreRoutingEngine;
import io.shardingsphere.core.routing.type.standard.StandardRoutingEngine;
import io.shardingsphere.core.routing.type.unicast.UnicastRoutingEngine;
import io.shardingsphere.core.rule.ShardingRule;
import io.shardingsphere.core.rule.TableRule;
import io.shardingsphere.core.util.SQLLogger;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Sharding router with parse.
 * 
 * @author zhangiang
 * @author maxiaoguang
 * @author panjuan
 */
@RequiredArgsConstructor
public final class ParsingSQLRouter implements ShardingRouter {
    
    private final ShardingRule shardingRule;
    
    private final ShardingMetaData shardingMetaData;
    
    private final DatabaseType databaseType;
    
    private final boolean showSQL;
    
    private final List<Number> generatedKeys = new LinkedList<>();
    
    @Override
    public SQLStatement parse(final String logicSQL, final boolean useCache) {
        return new SQLParsingEngine(databaseType, logicSQL, shardingRule, shardingMetaData).parse(useCache);
    }
    
    @Override
    public SQLRouteResult route(final String logicSQL, final List<Object> parameters, final SQLStatement sqlStatement) {
        GeneratedKey generatedKey = null;
        if (sqlStatement instanceof InsertStatement) {
            generatedKey = getGenerateKey(shardingRule, (InsertStatement) sqlStatement, parameters);
        }

        SQLRouteResult result = new SQLRouteResult(sqlStatement, generatedKey);
        ShardingConditions shardingConditions = OptimizeEngineFactory.newInstance(shardingRule, sqlStatement, parameters, generatedKey).optimize();
        if (null != generatedKey) {
            setGeneratedKeys(result, generatedKey);
        }
        //先进行route 得到路由结果
        RoutingResult routingResult = route(parameters, sqlStatement, shardingConditions);
        //创建重写引擎
        SQLRewriteEngine rewriteEngine = new SQLRewriteEngine(shardingRule, logicSQL, databaseType, sqlStatement, shardingConditions, parameters);
        boolean isSingleRouting = routingResult.isSingleRouting();

        //如果是SelectStatement 且Limit分页参数不为空 则处理Limit
        if (sqlStatement instanceof SelectStatement && null != ((SelectStatement) sqlStatement).getLimit()) {

            processLimit(parameters, (SelectStatement) sqlStatement, isSingleRouting);
        }
        //对sql进行重写
        SQLBuilder sqlBuilder = rewriteEngine.rewrite(!isSingleRouting);

        for (TableUnit each : routingResult.getTableUnits().getTableUnits()) {
            result.getExecutionUnits().add(new SQLExecutionUnit(each.getDataSourceName(), rewriteEngine.generateSQL(each, sqlBuilder)));
        }
        if (showSQL) {
            SQLLogger.logSQL(logicSQL, sqlStatement, result.getExecutionUnits());
        }
        return result;
    }
    
    private RoutingResult route(final List<Object> parameters, final SQLStatement sqlStatement, final ShardingConditions shardingConditions) {
        Collection<String> tableNames = sqlStatement.getTables().getTableNames();
        RoutingEngine routingEngine;
        if (sqlStatement instanceof UseStatement) {
            routingEngine = new IgnoreRoutingEngine();
        } else if (sqlStatement instanceof DDLStatement) {
            routingEngine = new TableBroadcastRoutingEngine(shardingRule, sqlStatement);
        } else if (sqlStatement instanceof ShowDatabasesStatement || sqlStatement instanceof ShowTablesStatement) {
            routingEngine = new DatabaseBroadcastRoutingEngine(shardingRule);
        } else if (shardingConditions.isAlwaysFalse()) {
            routingEngine = new UnicastRoutingEngine(shardingRule, tableNames);
        } else if (sqlStatement instanceof DALStatement) {
            routingEngine = new UnicastRoutingEngine(shardingRule, tableNames);
        } else if (tableNames.isEmpty() && sqlStatement instanceof SelectStatement) {
            routingEngine = new UnicastRoutingEngine(shardingRule, tableNames);
        } else if (tableNames.isEmpty()) {
            routingEngine = new DatabaseBroadcastRoutingEngine(shardingRule);
        } else if (1 == tableNames.size() || shardingRule.isAllBindingTables(tableNames) || shardingRule.isAllInDefaultDataSource(tableNames)) {
            routingEngine = new StandardRoutingEngine(shardingRule, tableNames.iterator().next(), shardingConditions);
        } else {
            // TODO config for cartesian set
            routingEngine = new ComplexRoutingEngine(shardingRule, parameters, tableNames, shardingConditions);
        }
        return routingEngine.route();
    }
    
    private GeneratedKey getGenerateKey(final ShardingRule shardingRule, final InsertStatement insertStatement, final List<Object> parameters) {
        GeneratedKey result = null;
        if (-1 != insertStatement.getGenerateKeyColumnIndex()) {
            for (GeneratedKeyCondition generatedKeyCondition : insertStatement.getGeneratedKeyConditions()) {
                if (null == result) {
                    result = new GeneratedKey(generatedKeyCondition.getColumn());
                }
                if (-1 == generatedKeyCondition.getIndex()) {
                    result.getGeneratedKeys().add(generatedKeyCondition.getValue());
                } else {
                    result.getGeneratedKeys().add((Number) parameters.get(generatedKeyCondition.getIndex()));
                }
            }
            return result;
        }
        String logicTableName = insertStatement.getTables().getSingleTableName();
        Optional<TableRule> tableRule = shardingRule.tryFindTableRuleByLogicTable(logicTableName);
        if (!tableRule.isPresent()) {
            return null;
        }
        Optional<Column> generateKeyColumn = shardingRule.getGenerateKeyColumn(logicTableName);
        if (generateKeyColumn.isPresent()) {
            result = new GeneratedKey(generateKeyColumn.get());
            for (int i = 0; i < insertStatement.getInsertValues().getInsertValues().size(); i++) {
                result.getGeneratedKeys().add(shardingRule.generateKey(logicTableName));
            }
        }
        return result;
    }
    
    private void setGeneratedKeys(final SQLRouteResult sqlRouteResult, final GeneratedKey generatedKey) {
        generatedKeys.addAll(generatedKey.getGeneratedKeys());
        sqlRouteResult.getGeneratedKey().getGeneratedKeys().clear();
        sqlRouteResult.getGeneratedKey().getGeneratedKeys().addAll(generatedKeys);
    }
    
    private void processLimit(final List<Object> parameters, final SelectStatement selectStatement, final boolean isSingleRouting) {
        if (isSingleRouting) {
            //单表route 则单表查询的结果就是符合要求的，直接清掉
            selectStatement.setLimit(null);
            return;
        }
        //是否要获取全部的数据？
        boolean isNeedFetchAll = (!selectStatement.getGroupByItems().isEmpty() || !selectStatement.getAggregationSelectItems().isEmpty()) && !selectStatement.isSameGroupByAndOrderByItems();
        //对参数进行处理，
        selectStatement.getLimit().processParameters(parameters, isNeedFetchAll);
    }
}

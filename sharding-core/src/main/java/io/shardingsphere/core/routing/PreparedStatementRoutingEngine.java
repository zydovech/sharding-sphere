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

package io.shardingsphere.core.routing;

import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.metadata.ShardingMetaData;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import io.shardingsphere.core.routing.router.masterslave.ShardingMasterSlaveRouter;
import io.shardingsphere.core.routing.router.sharding.ShardingRouter;
import io.shardingsphere.core.routing.router.sharding.ShardingRouterFactory;
import io.shardingsphere.core.rule.ShardingRule;

import java.util.List;

/**
 * PreparedStatement routing engine.
 * 
 * @author zhangliang
 * @author panjuan
 */
public final class PreparedStatementRoutingEngine {

    /**
     * 原来的sql
     */
    private final String logicSQL;

    /**
     * sharding 的路由器 见ParsingSQLRouter
     */
    private final ShardingRouter shardingRouter;
    /**
     * 主从的路由器
     */
    private final ShardingMasterSlaveRouter masterSlaveRouter;

    /**
     * 解析后的 sql语句
     */
    private SQLStatement sqlStatement;
    
    public PreparedStatementRoutingEngine(final String logicSQL, final ShardingRule shardingRule, final ShardingMetaData shardingMetaData, final DatabaseType databaseType, final boolean showSQL) {
        this.logicSQL = logicSQL;
        shardingRouter = ShardingRouterFactory.createSQLRouter(shardingRule, shardingMetaData, databaseType, showSQL);
        masterSlaveRouter = new ShardingMasterSlaveRouter(shardingRule.getMasterSlaveRules());
    }
    
    /**
     * SQL route.
     * 
     * <p>First routing time will parse SQL, after second time will reuse first parsed result.</p>
     * 
     * @param parameters parameters of SQL placeholder
     * @return route result
     */
    public SQLRouteResult route(final List<Object> parameters) {
        if (null == sqlStatement) {
            //如果还没有进行解析，则调用shardingRouter进行解析
            sqlStatement = shardingRouter.parse(logicSQL, true);
        }
        //先经过shardingRouter 进行路由。。
        return masterSlaveRouter.route(shardingRouter.route(logicSQL, parameters, sqlStatement));
    }
}

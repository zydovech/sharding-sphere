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

package io.shardingsphere.core.parsing.parser.sql;

import io.shardingsphere.core.constant.SQLType;
import io.shardingsphere.core.parsing.parser.context.condition.Conditions;
import io.shardingsphere.core.parsing.parser.context.table.Tables;
import io.shardingsphere.core.parsing.parser.token.SQLToken;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

/**
 * SQL statement abstract class.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
@Getter
@Setter
@ToString
public abstract class AbstractSQLStatement implements SQLStatement {
    /**
     * sql类型
     */
    private final SQLType type;
    /**
     * 表
     */
    private final Tables tables = new Tables();
    /**
     * 过滤条件
     * 只有对路由结果有影响的条件，才添加进数组
     */
    private final Conditions conditions = new Conditions();

    /**
     * SQL标记对象
     */
    private final List<SQLToken> sqlTokens = new LinkedList<>();
    /**
     * 占位符的标记，初始为0 后面解析的时候 遇到一个就加一，估计是这样的作用
     */
    private int parametersIndex;
    
    @Override
    public final SQLType getType() {
        return type;
    }
    
    @Override
    public int increaseParametersIndex() {
        return ++parametersIndex;
    }
}

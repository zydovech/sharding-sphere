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

package io.shardingsphere.core.parsing.parser.dialect.mysql.clause;

import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.parsing.lexer.LexerEngine;
import io.shardingsphere.core.parsing.lexer.dialect.mysql.MySQLKeyword;
import io.shardingsphere.core.parsing.lexer.token.Literals;
import io.shardingsphere.core.parsing.lexer.token.Symbol;
import io.shardingsphere.core.parsing.parser.clause.SQLClauseParser;
import io.shardingsphere.core.parsing.parser.context.limit.Limit;
import io.shardingsphere.core.parsing.parser.context.limit.LimitValue;
import io.shardingsphere.core.parsing.parser.exception.SQLParsingException;
import io.shardingsphere.core.parsing.parser.sql.dql.select.SelectStatement;
import io.shardingsphere.core.parsing.parser.token.OffsetToken;
import io.shardingsphere.core.parsing.parser.token.RowCountToken;
import lombok.RequiredArgsConstructor;

/**
 * Limit clause parser for MySQL.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class MySQLLimitClauseParser implements SQLClauseParser {
    
    private final LexerEngine lexerEngine;
    
    /**
     * Parse limit.
     * 
     * @param selectStatement select statement
     */
    public void parse(final SelectStatement selectStatement) {
        if (!lexerEngine.skipIfEqual(MySQLKeyword.LIMIT)) {
            //不是Limit的话 则直接返回
            return;
        }
        int valueIndex = -1;
        //获取当前Token的结束位置
        int valueBeginPosition = lexerEngine.getCurrentToken().getEndPosition();
        int value;
        boolean isParameterForValue = false;
        if (lexerEngine.equalAny(Literals.INT)) {
            //如果是个int值，则获取
            value = Integer.parseInt(lexerEngine.getCurrentToken().getLiterals());
            valueBeginPosition = valueBeginPosition - (value + "").length();
        } else if (lexerEngine.equalAny(Symbol.QUESTION)) {
            //如果是个占位符，则获取是第几个占位符
            valueIndex = selectStatement.getParametersIndex();
            value = -1;
            valueBeginPosition--;
            isParameterForValue = true;
        } else {
            throw new SQLParsingException(lexerEngine);
        }
        lexerEngine.nextToken();
        if (lexerEngine.skipIfEqual(Symbol.COMMA)) {
            //如果是逗号，则已经获取到全部的limit数据，可以直接返回了
            selectStatement.setLimit(getLimitWithComma(valueIndex, valueBeginPosition, value, isParameterForValue, selectStatement));
            return;
        }
        if (lexerEngine.skipIfEqual(MySQLKeyword.OFFSET)) {
            //如果是 limit 1 offset 10 的写法 也直接返回
            selectStatement.setLimit(getLimitWithOffset(valueIndex, valueBeginPosition, value, isParameterForValue, selectStatement));
            return;
        }

        //当只有limit 10 这种写法的时候，会到下面来进行解析
        if (isParameterForValue) {
            //若是占位符 则增加对应的index
            selectStatement.increaseParametersIndex();
        } else {
            //不然新增RowCountToken
            selectStatement.getSqlTokens().add(new RowCountToken(valueBeginPosition, value));
        }
        //创建Limit
        Limit limit = new Limit(DatabaseType.MySQL);
        limit.setRowCount(new LimitValue(value, valueIndex, false));
        selectStatement.setLimit(limit);
    }
    
    private Limit getLimitWithComma(final int index, final int valueBeginPosition, final int value, final boolean isParameterForValue, final SelectStatement selectStatement) {
        int rowCountBeginPosition = lexerEngine.getCurrentToken().getEndPosition();
        int rowCountValue;
        int rowCountIndex = -1;
        boolean isParameterForRowCount = false;
        if (lexerEngine.equalAny(Literals.INT)) {
            rowCountValue = Integer.parseInt(lexerEngine.getCurrentToken().getLiterals());
            rowCountBeginPosition = rowCountBeginPosition - (rowCountValue + "").length();
        } else if (lexerEngine.equalAny(Symbol.QUESTION)) {
            rowCountIndex = -1 == index ? selectStatement.getParametersIndex() : index + 1;
            rowCountValue = -1;
            rowCountBeginPosition--;
            isParameterForRowCount = true;
        } else {
            throw new SQLParsingException(lexerEngine);
        }
        lexerEngine.nextToken();

        if (isParameterForValue) {
            //如果是占位符的，则增加parametersIndex的值
            selectStatement.increaseParametersIndex();
        } else {
            //不是占位符的 则直接新增新的OffsetToken
            selectStatement.getSqlTokens().add(new OffsetToken(valueBeginPosition, value));
        }
        if (isParameterForRowCount) {
            //如果是占位符的，则增加parametersIndex的值
            selectStatement.increaseParametersIndex();
        } else {
            //不是占位符的 则直接新增新的RowCountToken
            selectStatement.getSqlTokens().add(new RowCountToken(rowCountBeginPosition, rowCountValue));
        }
        //创建Limit
        Limit result = new Limit(DatabaseType.MySQL);
        result.setRowCount(new LimitValue(rowCountValue, rowCountIndex, false));
        result.setOffset(new LimitValue(value, index, true));
        return result;
    }
    
    private Limit getLimitWithOffset(final int index, final int valueBeginPosition, final int value, final boolean isParameterForValue, final SelectStatement selectStatement) {
        int offsetBeginPosition = lexerEngine.getCurrentToken().getEndPosition();
        int offsetValue = -1;
        int offsetIndex = -1;
        boolean isParameterForOffset = false;
        if (lexerEngine.equalAny(Literals.INT)) {
            offsetValue = Integer.parseInt(lexerEngine.getCurrentToken().getLiterals());
            offsetBeginPosition = offsetBeginPosition - (offsetValue + "").length();
        } else if (lexerEngine.equalAny(Symbol.QUESTION)) {
            offsetIndex = -1 == index ? selectStatement.getParametersIndex() : index + 1;
            offsetBeginPosition--;
            isParameterForOffset = true;
        } else {
            throw new SQLParsingException(lexerEngine);
        }
        lexerEngine.nextToken();
        if (isParameterForOffset) {
            selectStatement.increaseParametersIndex();
        } else {
            selectStatement.getSqlTokens().add(new OffsetToken(offsetBeginPosition, offsetValue));
        }
        if (isParameterForValue) {
            selectStatement.increaseParametersIndex();
        } else {
            selectStatement.getSqlTokens().add(new RowCountToken(valueBeginPosition, value));
        }
        Limit result = new Limit(DatabaseType.MySQL);
        result.setRowCount(new LimitValue(value, index, false));
        result.setOffset(new LimitValue(offsetValue, offsetIndex, true));
        return result;
    }
}

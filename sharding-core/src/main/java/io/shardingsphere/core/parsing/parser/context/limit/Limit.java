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

package io.shardingsphere.core.parsing.parser.context.limit;

import io.shardingsphere.core.constant.DatabaseType;
import io.shardingsphere.core.parsing.parser.exception.SQLParsingException;
import io.shardingsphere.core.util.NumberUtil;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

/**
 * Limit object.
 *
 * @author zhangliang
 * @author caohao
 */
@RequiredArgsConstructor
@Getter
@Setter
@ToString
public final class Limit {
    
    private final DatabaseType databaseType;
    
    private LimitValue offset;
    
    private LimitValue rowCount;
    
    /**
     * Get offset value.
     * 
     * @return offset value
     */
    public int getOffsetValue() {
        return null != offset ? offset.getValue() : 0;
    }
    
    /**
     * Get row count value.
     *
     * @return row count value
     */
    public int getRowCountValue() {
        return null != rowCount ? rowCount.getValue() : -1;
    }
    
    /**
     * Fill parameters for rewrite limit.
     *
     * @param parameters parameters
     * @param isFetchAll is fetch all data or not
     */
    public void processParameters(final List<Object> parameters, final boolean isFetchAll) {
        //填充offset和count值到Limit中 ， 记录了原始的offset和count值
        fill(parameters);
        //重写传递进来的参数
        rewrite(parameters, isFetchAll);
    }
    
    private void fill(final List<Object> parameters) {
        int offset = 0;
        if (null != this.offset) {
            //如果index为-1 则代表不是占位符的情况，可以直接获取offset的值，否则从parameters中获取
            offset = -1 == this.offset.getIndex() ? getOffsetValue() : NumberUtil.roundHalfUp(parameters.get(this.offset.getIndex()));
            //设置对的值
            this.offset.setValue(offset);
        }
        int rowCount = 0;
        if (null != this.rowCount) {
            rowCount = -1 == this.rowCount.getIndex() ? getRowCountValue() : NumberUtil.roundHalfUp(parameters.get(this.rowCount.getIndex()));
            this.rowCount.setValue(rowCount);
        }
        if (offset < 0 || rowCount < 0) {
            //不允许出现负值
            throw new SQLParsingException("LIMIT offset and row count can not be a negative value.");
        }
    }
    
    private void rewrite(final List<Object> parameters, final boolean isFetchAll) {
        int rewriteOffset = 0;
        int rewriteRowCount;
        if (isFetchAll) {
            //如果获取全部，则count值为Integer.MAX_VALUE
            rewriteRowCount = Integer.MAX_VALUE;
        } else if (isNeedRewriteRowCount()) {
            //如果是mysql数据库 则需要重写rowCount的值，重写为 之前的 offset + count
            rewriteRowCount = null == rowCount ? -1 : getOffsetValue() + rowCount.getValue();
        } else {
            //不需要重写的话，则直接设置
            rewriteRowCount = rowCount.getValue();
        }
        //更改parameters中的值，offset变为0
        if (null != offset && offset.getIndex() > -1) {
            parameters.set(offset.getIndex(), rewriteOffset);
        }
        //更改parameters中的值，count变为offset+count
        if (null != rowCount && rowCount.getIndex() > -1) {
            parameters.set(rowCount.getIndex(), rewriteRowCount);
        }
    }
    
    /**
     * Is need rewrite row count.
     * 
     * @return is need rewrite row count or not
     */
    public boolean isNeedRewriteRowCount() {
        return DatabaseType.MySQL == databaseType || DatabaseType.PostgreSQL == databaseType || DatabaseType.H2 == databaseType;
    }
}

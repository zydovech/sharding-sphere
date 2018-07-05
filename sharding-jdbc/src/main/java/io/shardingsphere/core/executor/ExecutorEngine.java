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

package io.shardingsphere.core.executor;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.shardingsphere.core.constant.SQLType;
import io.shardingsphere.core.executor.event.AbstractExecutionEvent;
import io.shardingsphere.core.executor.event.DMLExecutionEvent;
import io.shardingsphere.core.executor.event.DQLExecutionEvent;
import io.shardingsphere.core.executor.event.EventExecutionType;
import io.shardingsphere.core.executor.event.OverallExecutionEvent;
import io.shardingsphere.core.executor.threadlocal.ExecutorDataMap;
import io.shardingsphere.core.executor.threadlocal.ExecutorExceptionHandler;
import io.shardingsphere.core.util.EventBusInstance;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * SQL execute engine.
 * 
 * @author gaohongtao
 * @author zhangliang
 * @author maxiaoguang
 */
@Slf4j
public final class ExecutorEngine implements AutoCloseable {
    
    private static final ThreadPoolExecutor SHUTDOWN_EXECUTOR = new ThreadPoolExecutor(
            0, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(10), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Sharding-JDBC-ExecutorEngineCloseTimer").build());
    
    @Getter
    private final ListeningExecutorService executorService;
    
    public ExecutorEngine(final int executorSize) {
        //创建executorService 线程池，用于多路同时执行数据库操作。ShardingDataSource负责创建ExecutorEngine
        executorService = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(
                executorSize, executorSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Sharding-JDBC-%d").build()));

        MoreExecutors.addDelayedShutdownHook(executorService, 60, TimeUnit.SECONDS);
    }
    
    /**
     * Execute.
     *
     * @param sqlType SQL type
     * @param baseStatementUnits statement execute unitS
     * @param executeCallback prepared statement execute callback
     * @param <T> class type of return value
     * @return execute result
     * @throws SQLException SQL exception
     */
    public <T> List<T> execute(
            final SQLType sqlType, final Collection<? extends BaseStatementUnit> baseStatementUnits, final ExecuteCallback<T> executeCallback) throws SQLException {
        if (baseStatementUnits.isEmpty()) {
            return Collections.emptyList();
        }
        OverallExecutionEvent event = new OverallExecutionEvent(sqlType, baseStatementUnits.size());
        //这个地方的Post 没有响应
        EventBusInstance.getInstance().post(event);
        Iterator<? extends BaseStatementUnit> iterator = baseStatementUnits.iterator();

        //下面使用多个线程 分开去执行不同BaseStatementUnit的数据库操作
        //获取第一个任务
        BaseStatementUnit firstInput = iterator.next();
        //交给线程池 执行下面所有任务 所有异步执行的结果 封装为了一个ListenableFuture
        ListenableFuture<List<T>> restFutures = asyncExecute(sqlType, Lists.newArrayList(iterator), executeCallback);
        T firstOutput;
        List<T> restOutputs;
        try {
            //同步执行第一个
            firstOutput = syncExecute(sqlType, firstInput, executeCallback);
            restOutputs = restFutures.get();
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            event.setException(ex);
            event.setEventExecutionType(EventExecutionType.EXECUTE_FAILURE);
            EventBusInstance.getInstance().post(event);
            ExecutorExceptionHandler.handleException(ex);
            return null;
        }
        event.setEventExecutionType(EventExecutionType.EXECUTE_SUCCESS);
        EventBusInstance.getInstance().post(event);
        List<T> result = Lists.newLinkedList(restOutputs);
        result.add(0, firstOutput);
        return result;
    }
    
    private <T> ListenableFuture<List<T>> asyncExecute(
            final SQLType sqlType, final Collection<BaseStatementUnit> baseStatementUnits, final ExecuteCallback<T> executeCallback) {
        //有多少个执行单元 就有多少个结果
        List<ListenableFuture<T>> result = new ArrayList<>(baseStatementUnits.size());
        //先获取是否抛出异常
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        final Map<String, Object> dataMap = ExecutorDataMap.getDataMap();
        for (final BaseStatementUnit each : baseStatementUnits) {
            //遍历 执行所有的可执行单元。像executorService中提交任务。executorService的线程池大小可以由参数设定，默认为可用的CPU个数，
            //executorService.submit 返回的是ListenableFuture
            result.add(executorService.submit(new Callable<T>() {
                
                @Override
                public T call() throws Exception {
                    //异步执行最后也是通过executeInternal 进行
                    return executeInternal(sqlType, each, executeCallback, isExceptionThrown, dataMap);
                }
            }));
        }
        return Futures.allAsList(result);
    }
    
    private <T> T syncExecute(final SQLType sqlType, final BaseStatementUnit baseStatementUnit, final ExecuteCallback<T> executeCallback) throws Exception {
        return executeInternal(sqlType, baseStatementUnit, executeCallback, ExecutorExceptionHandler.isExceptionThrown(), ExecutorDataMap.getDataMap());
    }
    
    private <T> T executeInternal(final SQLType sqlType, final BaseStatementUnit baseStatementUnit, final ExecuteCallback<T> executeCallback,
                                  final boolean isExceptionThrown, final Map<String, Object> dataMap) throws Exception {
        //进行同步。。
        synchronized (baseStatementUnit.getStatement().getConnection()) {
            T result;
            //设置是否抛出异常
            ExecutorExceptionHandler.setExceptionThrown(isExceptionThrown);
            //dataMap目前不知道干啥的 20180628
            ExecutorDataMap.setDataMap(dataMap);
            //用于记录所有的事件
            List<AbstractExecutionEvent> events = new LinkedList<>();
            for (List<Object> each : baseStatementUnit.getSqlExecutionUnit().getSqlUnit().getParameterSets()) {
                //添加DQLExecutionEvent 和 DMLExecutionEvent 事件
                events.add(getExecutionEvent(sqlType, baseStatementUnit, each));
            }
            for (AbstractExecutionEvent event : events) {
                //发布所有的事件
                EventBusInstance.getInstance().post(event);
            }
            try {
                //执行相关的操作 这里基本上就是直接调用jdbc相关的接口，进行调用
                result = executeCallback.execute(baseStatementUnit);
            } catch (final SQLException ex) {
                //捕获所有的SQLException 异常 如果出现其他异常，直接把当前线程打跪了 该怎么办？
                for (AbstractExecutionEvent each : events) {
                    //对执行sql上述的事件 发布执行失败
                    each.setEventExecutionType(EventExecutionType.EXECUTE_FAILURE);
                    each.setException(ex);
                    EventBusInstance.getInstance().post(each);
                    ExecutorExceptionHandler.handleException(ex);
                }
                return null;
            }
            for (AbstractExecutionEvent each : events) {
                each.setEventExecutionType(EventExecutionType.EXECUTE_SUCCESS);
                EventBusInstance.getInstance().post(each);
            }
            return result;
        }
    }
    
    private AbstractExecutionEvent getExecutionEvent(final SQLType sqlType, final BaseStatementUnit baseStatementUnit, final List<Object> parameters) {
        AbstractExecutionEvent result;
        if (SQLType.DQL == sqlType) {
            result = new DQLExecutionEvent(baseStatementUnit.getSqlExecutionUnit().getDataSource(), baseStatementUnit.getSqlExecutionUnit().getSqlUnit(), parameters);
        } else {
            result = new DMLExecutionEvent(baseStatementUnit.getSqlExecutionUnit().getDataSource(), baseStatementUnit.getSqlExecutionUnit().getSqlUnit(), parameters);
        }
        return result;
    }
    
    @Override
    public void close() {
        SHUTDOWN_EXECUTOR.execute(new Runnable() {
            
            @Override
            public void run() {
                try {
                    executorService.shutdown();
                    while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (final InterruptedException ex) {
                    log.error("ExecutorEngine can not been terminated", ex);
                }
            }
        });
    }
}

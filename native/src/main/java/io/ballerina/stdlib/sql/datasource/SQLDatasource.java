/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.ballerina.stdlib.sql.datasource;

import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.BallerinaTransactionContext;
import io.ballerina.runtime.transactions.TransactionLocalContext;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.transaction.SQLTransactionContext;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

/**
 * SQL datasource representation.
 *
 * @since 1.2.0
 */
public class SQLDatasource {

    private AtomicInteger clientCounter = new AtomicInteger(0);
    private Lock mutex = new ReentrantLock();
    private boolean poolShutdown = false;
    private boolean xaConn;
    private AtomikosDataSourceBean atomikosDataSourceBean;
    private HikariDataSource hikariDataSource;
    private XADataSource xaDataSource;
    private boolean executeGKFlag;
    private boolean batchExecuteGKFlag;
    private static final String POOL_MAP_KEY = UUID.randomUUID().toString();

    private SQLDatasource(SQLDatasourceParams sqlDatasourceParams) {

        Connection connection = null;
        try {
            if (sqlDatasourceParams.datasourceName != null && !sqlDatasourceParams.datasourceName.isEmpty() &&
                    TransactionResourceManager.getInstance().getTransactionManagerEnabled()) {
                Class<?> dataSourceClass =
                        ClassLoader.getSystemClassLoader().loadClass(sqlDatasourceParams.datasourceName);
                if (XADataSource.class.isAssignableFrom(dataSourceClass)) {
                    xaConn = true;
                    atomikosDataSourceBean = buildXAAwareDataSource(sqlDatasourceParams);
                    connection = getConnection();
                    return;
                }
            }
            hikariDataSource = buildNonXADataSource(sqlDatasourceParams);
            if (hikariDataSource.isWrapperFor(XADataSource.class)) {
                xaConn = true;
                xaDataSource = hikariDataSource.unwrap(XADataSource.class);
                connection = xaDataSource.getXAConnection().getConnection();
                return;
            }
            connection = getConnection();
        } catch (SQLException e) {
            throw ErrorGenerator.getSQLDatabaseError(e,
                    "error while verifying the connection for " + Constants.CONNECTOR_NAME + ", ");
        } catch (ClassNotFoundException e) {
            throw ErrorGenerator.getSQLApplicationError("error while loading datasource class for " +
                    Constants.CONNECTOR_NAME + ", ");
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    public static synchronized Map<PoolKey, SQLDatasource> putDatasourceContainer(
            BMap<BString, Object> poolOptions,
            ConcurrentHashMap<PoolKey, SQLDatasource> datasourceMap) {
        Map<PoolKey, SQLDatasource> existingDataSourceMap =
                (Map<PoolKey, SQLDatasource>) poolOptions.getNativeData(POOL_MAP_KEY);
        if (existingDataSourceMap != null) {
            return existingDataSourceMap;
        }
        poolOptions.addNativeData(POOL_MAP_KEY, datasourceMap);
        return datasourceMap;
    }

    /**
     * Retrieve the {@link SQLDatasource}} object corresponding to the provided  URL in
     * {@link SQLDatasource.SQLDatasourceParams}.
     * Creates a datasource if it doesn't exist.
     *
     * @param sqlDatasourceParams datasource parameters required to retrieve the JDBC URL for datasource lookup and
     *                            initialization of the newly created datasource if it doesn't exists
     * @return The existing or newly created {@link SQLDatasource} object
     */
    public static SQLDatasource retrieveDatasource(SQLDatasource.SQLDatasourceParams sqlDatasourceParams,
                                                   boolean executeGKFlag, boolean batchExecuteGKFlag) {
        PoolKey poolKey = new PoolKey(sqlDatasourceParams.url, sqlDatasourceParams.options);
        Map<PoolKey, SQLDatasource> hikariDatasourceMap = (Map<PoolKey, SQLDatasource>) sqlDatasourceParams
                .connectionPool.getNativeData(POOL_MAP_KEY);
        // map could be null only in a local pool creation scenario
        if (hikariDatasourceMap == null) {
            hikariDatasourceMap = putDatasourceContainer(sqlDatasourceParams.connectionPool, new ConcurrentHashMap<>());
        }
        SQLDatasource existingSqlDatasource = hikariDatasourceMap.get(poolKey);
        SQLDatasource sqlDatasourceToBeReturned = existingSqlDatasource;
        if (existingSqlDatasource != null) {
            existingSqlDatasource.acquireMutex();
            try {
                if (!existingSqlDatasource.isPoolShutdown()) {
                    existingSqlDatasource.incrementClientCounter();
                } else {
                    sqlDatasourceToBeReturned = hikariDatasourceMap.compute(poolKey,
                            (key, value) -> createAndInitDatasource(sqlDatasourceParams));
                }
            } finally {
                existingSqlDatasource.releaseMutex();
            }
        } else {
            sqlDatasourceToBeReturned = hikariDatasourceMap.computeIfAbsent(poolKey,
                    key -> createAndInitDatasource(sqlDatasourceParams));

        }
        sqlDatasourceToBeReturned.setExecuteGKFlag(executeGKFlag);
        sqlDatasourceToBeReturned.setBatchExecuteGKFlag(batchExecuteGKFlag);
        return sqlDatasourceToBeReturned;
    }

    public static Connection getConnection(boolean isInTrx, TransactionResourceManager trxResourceManager,
                                           BObject client, SQLDatasource datasource) throws SQLException {
        Connection conn;
        try {
            if (!isInTrx) {
                return datasource.getConnection();
            }
            String connectorId = (String) client.getNativeData(Constants.SQL_CONNECTOR_TRANSACTION_ID);
            boolean isXAConnection = datasource.isXADataSource();
            TransactionLocalContext transactionLocalContext = trxResourceManager.getCurrentTransactionContext();
            String globalTxId = transactionLocalContext.getGlobalTransactionId();
            String currentTxBlockId = transactionLocalContext.getCurrentTransactionBlockId();
            BallerinaTransactionContext txContext = transactionLocalContext.getTransactionContext(connectorId);
            if (txContext == null) {
                if (isXAConnection && !trxResourceManager.getTransactionManagerEnabled()) {
                    XAConnection xaConn = datasource.getXAConnection();
                    XAResource xaResource = xaConn.getXAResource();
                    TransactionResourceManager.getInstance()
                            .beginXATransaction(globalTxId, currentTxBlockId, xaResource);
                    conn = xaConn.getConnection();
                    txContext = new SQLTransactionContext(conn, xaResource);
                } else if (isXAConnection) {
                    TransactionResourceManager.getInstance()
                            .beginXATransaction(globalTxId, currentTxBlockId, null);
                    conn = datasource.getConnection();
                    conn.setAutoCommit(false);
                    txContext = new SQLTransactionContext(conn);
                } else {
                    conn = datasource.getConnection();
                    conn.setAutoCommit(false);
                    txContext = new SQLTransactionContext(conn);
                }
                transactionLocalContext.registerTransactionContext(connectorId, txContext);
                TransactionResourceManager.getInstance().register(globalTxId, currentTxBlockId, txContext);
            } else {
                conn = ((SQLTransactionContext) txContext).getConnection();
            }
        } catch (SQLException e) {
            throw new SQLException("error while getting the connection for " + Constants.CONNECTOR_NAME + ". "
                    + e.getMessage(), e.getSQLState(), e.getErrorCode());
        }
        return conn;
    }

    public static SQLDatasourceParams createSQLDatasourceParams(BMap<BString, Object> sqlDatasourceParams,
                                                                BMap<BString, Object> globalConnectionPool) {
        BMap<BString, Object> connPoolProps = (BMap<BString, Object>) sqlDatasourceParams
                .getMapValue(Constants.SQLParamsFields.CONNECTION_POOL_OPTIONS);
        Properties poolProperties = null;
        if (connPoolProps != null) {
            poolProperties = new Properties();
            for (BString key : connPoolProps.getKeys()) {
                poolProperties.setProperty(key.getValue(), connPoolProps.getStringValue(key).getValue());
            }
        }
        BString userVal = sqlDatasourceParams.getStringValue(Constants.SQLParamsFields.USER);
        String user = userVal == null ? null : userVal.getValue();
        BString passwordVal = sqlDatasourceParams.getStringValue(Constants.SQLParamsFields.PASSWORD);
        String password = passwordVal == null ? null : passwordVal.getValue();
        BString dataSourceNamVal = sqlDatasourceParams.getStringValue(Constants.SQLParamsFields.DATASOURCE_NAME);
        String datasourceName = dataSourceNamVal == null ? null : dataSourceNamVal.getValue();
        return new SQLDatasource.SQLDatasourceParams()
                .setUrl(sqlDatasourceParams.getStringValue(Constants.SQLParamsFields.URL).getValue())
                .setUser(user)
                .setPassword(password)
                .setDatasourceName(datasourceName)
                .setOptions(sqlDatasourceParams.getMapValue(Constants.SQLParamsFields.OPTIONS))
                .setConnectionPool(sqlDatasourceParams.getMapValue(Constants.SQLParamsFields.CONNECTION_POOL),
                        globalConnectionPool)
                .setPoolProperties(poolProperties);
    }

    private static SQLDatasource createAndInitDatasource(SQLDatasource.SQLDatasourceParams sqlDatasourceParams) {
        SQLDatasource newSqlDatasource = new SQLDatasource(sqlDatasourceParams);
        newSqlDatasource.incrementClientCounter();
        return newSqlDatasource;
    }

    private Connection getConnection() throws SQLException {
        if (atomikosDataSourceBean != null) {
            return atomikosDataSourceBean.getConnection();
        }
        return hikariDataSource.getConnection();
    }

    private XAConnection getXAConnection() throws SQLException {
        if (isXADataSource()) {
            return xaDataSource.getXAConnection();
        }
        return null;
    }

    private boolean isXADataSource() {
        return xaConn;
    }

    private void closeConnectionPool() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }

        if (atomikosDataSourceBean != null) {
            atomikosDataSourceBean.close();
        }
        poolShutdown = true;
    }

    private boolean isPoolShutdown() {
        return poolShutdown;
    }

    private void incrementClientCounter() {
        clientCounter.incrementAndGet();
    }

    public void decrementClientCounterAndAttemptPoolShutdown() {
        acquireMutex();
        if (!poolShutdown) {
            if (clientCounter.decrementAndGet() == 0) {
                closeConnectionPool();
            }
        }
        releaseMutex();
    }

    private void releaseMutex() {
        mutex.unlock();
    }

    private void acquireMutex() {
        mutex.lock();
    }

    private HikariDataSource buildNonXADataSource(SQLDatasourceParams sqlDatasourceParams) {
        try {
            HikariDataSource hikariDataSource;
            HikariConfig config;
            if (sqlDatasourceParams.poolProperties != null) {
                config = new HikariConfig(sqlDatasourceParams.poolProperties);
            } else {
                config = new HikariConfig();
            }
            config.setJdbcUrl(sqlDatasourceParams.url);
            config.setUsername(sqlDatasourceParams.user);
            config.setPassword(sqlDatasourceParams.password);
            if (sqlDatasourceParams.datasourceName != null && !sqlDatasourceParams.datasourceName.isEmpty()) {
                if (sqlDatasourceParams.options == null || !sqlDatasourceParams.options
                        .containsKey(Constants.Options.URL)) {
                    //It is required to set the url to the datasource property when the
                    //datasource class name is provided. Because according to hikari
                    //either jdbcUrl or datasourceClassName will be honoured.
                    config.addDataSourceProperty(Constants.Options.URL.getValue(), sqlDatasourceParams.url);
                }
                if (sqlDatasourceParams.user != null) {
                    config.addDataSourceProperty(Constants.USERNAME, sqlDatasourceParams.user);
                }
                if (sqlDatasourceParams.password != null) {
                    config.addDataSourceProperty(Constants.PASSWORD, sqlDatasourceParams.password);
                }
            }
            config.setDataSourceClassName(sqlDatasourceParams.datasourceName);
            if (sqlDatasourceParams.connectionPool != null) {
                int maxOpenConn = sqlDatasourceParams.connectionPool.
                        getIntValue(Constants.ConnectionPool.MAX_OPEN_CONNECTIONS).intValue();
                if (maxOpenConn >= 1) {
                    config.setMaximumPoolSize(maxOpenConn);
                } else {
                    throw new ApplicationError("ConnectionPool field 'maxOpenConnections' cannot be less than one.");
                }

                Object connLifeTimeSec = sqlDatasourceParams.connectionPool
                        .get(Constants.ConnectionPool.MAX_CONNECTION_LIFE_TIME);
                if (connLifeTimeSec instanceof BDecimal) {
                    BDecimal connLifeTime = (BDecimal) connLifeTimeSec;
                    if (connLifeTime.floatValue() >= 30) {
                        long connLifeTimeMS = Double.valueOf(connLifeTime.floatValue() * 1000).longValue();
                        config.setMaxLifetime(connLifeTimeMS);
                    } else {
                        // Here if the connection life time is minimum 30s, the default value will be used
                        throw new ApplicationError("ConnectionPool field 'maxConnectionLifeTime' cannot be less than 30s.");
                    }
                }
                int minIdleConnections = sqlDatasourceParams.connectionPool
                        .getIntValue(Constants.ConnectionPool.MIN_IDLE_CONNECTIONS).intValue();
                if (minIdleConnections > 0) {
                    config.setMinimumIdle(minIdleConnections);
                } else {
                    throw new ApplicationError("ConnectionPool field 'minIdleConnections' cannot be negative.");
                }
            }
            if (sqlDatasourceParams.options != null) {
                BMap<BString, Object> optionMap = (BMap<BString, Object>) sqlDatasourceParams.options;
                optionMap.entrySet().forEach(entry ->
                        config.addDataSourceProperty(entry.getKey().getValue(), entry.getValue())
                );
            }
            hikariDataSource = new HikariDataSource(config);
            Runtime.getRuntime().addShutdownHook(new Thread(this::closeConnectionPool));
            return hikariDataSource;
        } catch (Throwable t) {
            throw ErrorGenerator.getSQLApplicationError(buildErrorMessage(t));
        }
    }

    private AtomikosDataSourceBean buildXAAwareDataSource(SQLDatasourceParams sqlDatasourceParams) {
        AtomikosDataSourceBean atomikosDataSource = new AtomikosDataSourceBean();
        try {
            Properties xaProperties = new Properties();
            if (sqlDatasourceParams.datasourceName != null && !sqlDatasourceParams.datasourceName.isEmpty()) {
                if (sqlDatasourceParams.options == null || !sqlDatasourceParams.options
                        .containsKey(Constants.Options.URL)) {
                    xaProperties.setProperty(Constants.Options.URL.getValue(), sqlDatasourceParams.url);
                }
                if (sqlDatasourceParams.user != null) {
                    xaProperties.setProperty(Constants.USERNAME, sqlDatasourceParams.user);
                }
                if (sqlDatasourceParams.password != null) {
                    xaProperties.setProperty(Constants.PASSWORD, sqlDatasourceParams.password);
                }
            }
            if (sqlDatasourceParams.connectionPool != null) {
                int maxOpenConn = sqlDatasourceParams.connectionPool.
                        getIntValue(Constants.ConnectionPool.MAX_OPEN_CONNECTIONS).intValue();
                if (maxOpenConn > 0) {
                    atomikosDataSource.setMaxPoolSize(maxOpenConn);
                }

                Object connLifeTimeSec = sqlDatasourceParams.connectionPool
                        .get(Constants.ConnectionPool.MAX_CONNECTION_LIFE_TIME);
                if (connLifeTimeSec instanceof BDecimal) {
                    BDecimal connLifeTime = (BDecimal) connLifeTimeSec;
                    if (connLifeTime.floatValue() > 0) {
                        atomikosDataSource.setMaxLifetime(Double.valueOf(connLifeTime.floatValue()).intValue());
                    }
                }
            }
            if (sqlDatasourceParams.options != null) {
                BMap<BString, Object> optionMap = (BMap<BString, Object>) sqlDatasourceParams.options;
                optionMap.entrySet().forEach(entry ->
                        xaProperties.setProperty(entry.getKey().getValue(), entry.getValue().toString())
                );
            }

            atomikosDataSource.setXaProperties(xaProperties);
            atomikosDataSource.setUniqueResourceName(UUID.randomUUID().toString());
            atomikosDataSource.setXaDataSourceClassName(sqlDatasourceParams.datasourceName);
            Runtime.getRuntime().addShutdownHook(new Thread(this::closeConnectionPool));
            return atomikosDataSource;
        } catch (Throwable t) {
            throw ErrorGenerator.getSQLApplicationError(buildErrorMessage(t));
        }
    }

    private String buildErrorMessage(Throwable t) {
        if (t.getCause() instanceof ClassNotFoundException) {
            return "Error while loading database driver. This may be because the database driver path is " +
                    "not configured correctly in the `Ballerina.toml` file or provided database driver " +
                    "version is not supported by the connector";
        }
        StringBuilder message = new StringBuilder("Error in SQL connector configuration: " + t.getMessage() + "");
        String lastCauseMessage;
        int count = 0;
        while (t.getCause() != null && count < 3) {
            lastCauseMessage = t.getCause().getMessage();
            message.append(" Caused by :").append(lastCauseMessage);
            count++;
            t = t.getCause();
        }
        return message.toString();
    }

    private void setExecuteGKFlag(boolean flag) {
        this.executeGKFlag = flag;
    }

    private void setBatchExecuteGKFlag(boolean flag) {
        this.batchExecuteGKFlag = flag;
    }

    public boolean getExecuteGKFlag() {
        return this.executeGKFlag;
    }

    public boolean getBatchExecuteGKFlag() {
        return this.batchExecuteGKFlag;
    }

    /**
     * This class encapsulates the parameters required for the initialization of {@code SQLDatasource} class.
     */
    public static class SQLDatasourceParams {
        private String url;
        private String user;
        private String password;
        private String datasourceName;
        private BMap connectionPool = null;
        private BMap options;
        private Properties poolProperties;

        public SQLDatasourceParams() {
        }

        public SQLDatasourceParams setConnectionPool(BMap connectionPool, BMap globalConnectionPool) {
            if (connectionPool != null) {
                this.connectionPool = connectionPool;
            } else {
                this.connectionPool = globalConnectionPool;
            }
            return this;
        }

        public SQLDatasourceParams setUrl(String url) {
            this.url = url;
            return this;
        }

        public SQLDatasourceParams setUser(String user) {
            this.user = user;
            return this;
        }

        public SQLDatasourceParams setPassword(String password) {
            this.password = password;
            return this;
        }

        public SQLDatasourceParams setDatasourceName(String datasourceName) {
            this.datasourceName = datasourceName;
            return this;
        }

        public SQLDatasourceParams setOptions(BMap options) {
            this.options = options;
            return this;
        }

        public SQLDatasourceParams setPoolProperties(Properties properties) {
            this.poolProperties = properties;
            return this;
        }
    }
}

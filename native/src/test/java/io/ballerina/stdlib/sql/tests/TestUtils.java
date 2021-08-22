/*
 *  Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.ballerina.stdlib.sql.tests;

import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.ObjectType;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BFuture;
import io.ballerina.runtime.api.values.BInitialValueEntry;
import io.ballerina.runtime.api.values.BIterator;
import io.ballerina.runtime.api.values.BLink;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.runtime.internal.IteratorUtils;
import io.ballerina.runtime.internal.scheduling.Strand;
import io.ballerina.runtime.internal.types.BField;
import io.ballerina.runtime.internal.types.BRecordType;
import io.ballerina.runtime.internal.values.BmpStringValue;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * TestUtils class for testing ballerina native classes/methods.
 */
public class TestUtils {

    /**
     * ExtendedColumnDefinition Class for test utils.
     */
    public static class ExtendedColumnDefinition extends ColumnDefinition {

        public ExtendedColumnDefinition(String columnName, String ballerinaFieldName, int sqlType, String sqlName,
                                        Type ballerinaType, boolean isNullable) {
            super(columnName, ballerinaFieldName, sqlType, sqlName, ballerinaType, isNullable);
        }
    }

    private static Module emptyModule = new Module(null, null, null);

    public static BObject getMockObject(String name) {
        return new BObject() {
            HashMap<String, Object> nativeData = new HashMap<>();

            @Override
            public Object call(Strand strand, String s, Object... objects) {
                return null;
            }

            @Override
            public BFuture start(Strand strand, String s, Object... objects) {
                return null;
            }

            @Override
            public ObjectType getType() {
                return TypeCreator.createObjectType(name, emptyModule, 0);
            }

            @Override
            public Object get(BString bString) {
                return null;
            }

            @Override
            public long getIntValue(BString bString) {
                return 0;
            }

            @Override
            public double getFloatValue(BString bString) {
                return 0;
            }

            @Override
            public BString getStringValue(BString bString) {
                return null;
            }

            @Override
            public boolean getBooleanValue(BString bString) {
                return false;
            }

            @Override
            public BMap getMapValue(BString bString) {
                return null;
            }

            @Override
            public BObject getObjectValue(BString bString) {
                return null;
            }

            @Override
            public BArray getArrayValue(BString bString) {
                return null;
            }

            @Override
            public void addNativeData(String s, Object o) {
                nativeData.put(s, o);
            }

            @Override
            public Object getNativeData(String s) {
                return nativeData.get(s);
            }

            @Override
            public HashMap<String, Object> getNativeData() {
                return null;
            }

            @Override
            public void set(BString bString, Object o) {

            }

            @Override
            public Object copy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public Object frozenCopy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public int size() {
                return BObject.super.size();
            }

            @Override
            public boolean isFrozen() {
                return BObject.super.isFrozen();
            }

            @Override
            public void freezeDirect() {
                BObject.super.freezeDirect();
            }

            @Override
            public void serialize(OutputStream outputStream) {
                BObject.super.serialize(outputStream);
            }

            @Override
            public BTypedesc getTypedesc() {
                return null;
            }

            @Override
            public String stringValue(BLink bLink) {
                return null;
            }

            @Override
            public String informalStringValue(BLink parent) {
                return BObject.super.informalStringValue(parent);
            }

            @Override
            public String expressionStringValue(BLink bLink) {
                return null;
            }
        };
    }

    public static Connection getMockConnection(boolean isClosed) {
        return new Connection() {
            @Override
            public Statement createStatement() throws SQLException {
                return null;
            }

            @Override
            public PreparedStatement prepareStatement(String sql) throws SQLException {
                return null;
            }

            @Override
            public CallableStatement prepareCall(String sql) throws SQLException {
                return null;
            }

            @Override
            public String nativeSQL(String sql) throws SQLException {
                return null;
            }

            @Override
            public void setAutoCommit(boolean autoCommit) throws SQLException {

            }

            @Override
            public boolean getAutoCommit() throws SQLException {
                return false;
            }

            @Override
            public void commit() throws SQLException {
                throw new SQLException("Commit Error");
            }

            @Override
            public void rollback() throws SQLException {
                throw new SQLException("Rollback Error");
            }

            @Override
            public void close() throws SQLException {
                throw new SQLException("Close Error");
            }

            @Override
            public boolean isClosed() throws SQLException {
                return isClosed;
            }

            @Override
            public DatabaseMetaData getMetaData() throws SQLException {
                return null;
            }

            @Override
            public void setReadOnly(boolean readOnly) throws SQLException {

            }

            @Override
            public boolean isReadOnly() throws SQLException {
                return false;
            }

            @Override
            public void setCatalog(String catalog) throws SQLException {

            }

            @Override
            public String getCatalog() throws SQLException {
                return null;
            }

            @Override
            public void setTransactionIsolation(int level) throws SQLException {

            }

            @Override
            public int getTransactionIsolation() throws SQLException {
                return 0;
            }

            @Override
            public SQLWarning getWarnings() throws SQLException {
                return null;
            }

            @Override
            public void clearWarnings() throws SQLException {

            }

            @Override
            public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
                return null;
            }

            @Override
            public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                    throws SQLException {
                return null;
            }

            @Override
            public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                    throws SQLException {
                return null;
            }

            @Override
            public Map<String, Class<?>> getTypeMap() throws SQLException {
                return null;
            }

            @Override
            public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

            }

            @Override
            public void setHoldability(int holdability) throws SQLException {

            }

            @Override
            public int getHoldability() throws SQLException {
                return 0;
            }

            @Override
            public Savepoint setSavepoint() throws SQLException {
                return null;
            }

            @Override
            public Savepoint setSavepoint(String name) throws SQLException {
                return null;
            }

            @Override
            public void rollback(Savepoint savepoint) throws SQLException {

            }

            @Override
            public void releaseSavepoint(Savepoint savepoint) throws SQLException {

            }

            @Override
            public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                    throws SQLException {
                return null;
            }

            @Override
            public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                      int resultSetHoldability) throws SQLException {
                return null;
            }

            @Override
            public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                                 int resultSetHoldability) throws SQLException {
                return null;
            }

            @Override
            public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
                return null;
            }

            @Override
            public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
                return null;
            }

            @Override
            public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
                return null;
            }

            @Override
            public Clob createClob() throws SQLException {
                return null;
            }

            @Override
            public Blob createBlob() throws SQLException {
                return null;
            }

            @Override
            public NClob createNClob() throws SQLException {
                return null;
            }

            @Override
            public SQLXML createSQLXML() throws SQLException {
                return null;
            }

            @Override
            public boolean isValid(int timeout) throws SQLException {
                return false;
            }

            @Override
            public void setClientInfo(String name, String value) throws SQLClientInfoException {

            }

            @Override
            public void setClientInfo(Properties properties) throws SQLClientInfoException {

            }

            @Override
            public String getClientInfo(String name) throws SQLException {
                return null;
            }

            @Override
            public Properties getClientInfo() throws SQLException {
                return null;
            }

            @Override
            public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
                return null;
            }

            @Override
            public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
                return new Struct() {
                    @Override
                    public String getSQLTypeName() throws SQLException {
                        return typeName;
                    }

                    @Override
                    public Object[] getAttributes() throws SQLException {
                        return attributes;
                    }

                    @Override
                    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                        return new Object[0];
                    }
                };
            }

            @Override
            public void setSchema(String schema) throws SQLException {

            }

            @Override
            public String getSchema() throws SQLException {
                return null;
            }

            @Override
            public void abort(Executor executor) throws SQLException {

            }

            @Override
            public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

            }

            @Override
            public int getNetworkTimeout() throws SQLException {
                return 0;
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                return null;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return false;
            }
        };
    }

    public static XAResource getMockXAResource() {
        return new XAResource() {
            @Override
            public void commit(Xid xid, boolean onePhase) throws XAException {

            }

            @Override
            public void end(Xid xid, int flags) throws XAException {

            }

            @Override
            public void forget(Xid xid) throws XAException {

            }

            @Override
            public int getTransactionTimeout() throws XAException {
                return 0;
            }

            @Override
            public boolean isSameRM(XAResource xares) throws XAException {
                return false;
            }

            @Override
            public int prepare(Xid xid) throws XAException {
                return 0;
            }

            @Override
            public Xid[] recover(int flag) throws XAException {
                return new Xid[0];
            }

            @Override
            public void rollback(Xid xid) throws XAException {

            }

            @Override
            public boolean setTransactionTimeout(int seconds) throws XAException {
                return false;
            }

            @Override
            public void start(Xid xid, int flags) throws XAException {

            }
        };
    }

    public static BValue getMockBValueJson() {
        return new BValue() {
            @Override
            public Object copy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public Object frozenCopy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public String stringValue(BLink bLink) {
                return null;
            }

            @Override
            public String expressionStringValue(BLink bLink) {
                return null;
            }

            @Override
            public Type getType() {
                return PredefinedTypes.TYPE_JSON;
            }
        };
    }

    public static Field getField() {
        return new Field() {
            @Override
            public long getFlags() {
                return 0;
            }

            @Override
            public Type getFieldType() {
                return PredefinedTypes.TYPE_INT;
            }

            @Override
            public String getFieldName() {
                return null;
            }
        };
    }

    public static Struct getStruct() {
        return new Struct() {
            @Override
            public String getSQLTypeName() throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes() throws SQLException {
                return new String[]{"2", "2"};
            }

            @Override
            public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                return new Object[0];
            }
        };
    }

    public static Struct getDecimalStruct() {
        return new Struct() {
            @Override
            public String getSQLTypeName() throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes() throws SQLException {
                return new BigDecimal[]{new BigDecimal(1), new BigDecimal(2)};
            }

            @Override
            public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                return new Object[0];
            }
        };
    }

    public static Struct getFloatStruct() {
        return new Struct() {
            @Override
            public String getSQLTypeName() throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes() throws SQLException {
                return new Double[]{1.2, 2.3};
            }

            @Override
            public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                return new Object[0];
            }
        };
    }

    public static Struct getIntStruct() {
        return new Struct() {
            @Override
            public String getSQLTypeName() throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes() throws SQLException {
                return new Integer[]{ 1, 2};
            }

            @Override
            public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                return new Object[0];
            }
        };
    }

    public static Struct getBooleanStruct() {
        return new Struct() {
            @Override
            public String getSQLTypeName() throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes() throws SQLException {
                return new Integer[]{1, 0};
            }

            @Override
            public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                return new Object[0];
            }
        };
    }

    public static Struct getRecordStruct() {
        return new Struct() {
            @Override
            public String getSQLTypeName() throws SQLException {
                return null;
            }

            @Override
            public Object[] getAttributes() throws SQLException {
                return new Struct[]{getBooleanStruct()};
            }

            @Override
            public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
                return new Object[0];
            }
        };
    }

    public static RecordType getIntStructRecord() {
        Map<String, Field> fields = new HashMap();
        fields.put("value1", new BField(PredefinedTypes.TYPE_INT, "value1", 256L));
        fields.put("value2", new BField(PredefinedTypes.TYPE_INT, "value2", 256L));
        return new BRecordType("$$returnType$$", (Module) null, 0L, fields, (Type) null, true,
                IteratorUtils.getTypeFlags(PredefinedTypes.TYPE_INT));
    }

    public static RecordType getBooleanStructRecord() {
        Map<String, Field> fields = new HashMap();
        fields.put("value1", new BField(PredefinedTypes.TYPE_BOOLEAN, "value1", 256L));
        fields.put("value2", new BField(PredefinedTypes.TYPE_BOOLEAN, "value2", 256L));
        return new BRecordType("$$returnType$$", (Module) null, 0L, fields, (Type) null, true,
                IteratorUtils.getTypeFlags(PredefinedTypes.TYPE_BOOLEAN));
    }

    public static RecordType getFloatStructRecord() {
        Map<String, Field> fields = new HashMap();
        fields.put("value1", new BField(PredefinedTypes.TYPE_FLOAT, "value1", 256L));
        fields.put("value2", new BField(PredefinedTypes.TYPE_FLOAT, "value2", 256L));
        return new BRecordType("$$returnType$$", (Module) null, 0L, fields, (Type) null, true,
                IteratorUtils.getTypeFlags(PredefinedTypes.TYPE_FLOAT));
    }

    public static RecordType getStringStructRecord() {
        Map<String, Field> fields = new HashMap();
        fields.put("value1", new BField(PredefinedTypes.TYPE_STRING, "value1", 256L));
        fields.put("value2", new BField(PredefinedTypes.TYPE_STRING, "value2", 256L));
        return new BRecordType("$$returnType$$", (Module) null, 0L, fields, (Type) null, true,
                IteratorUtils.getTypeFlags(PredefinedTypes.TYPE_STRING));
    }

    public static RecordType getDecimalStructRecord() {
        Map<String, Field> fields = new HashMap();
        fields.put("value1", new BField(PredefinedTypes.TYPE_DECIMAL, "value1", 256L));
        fields.put("value2", new BField(PredefinedTypes.TYPE_DECIMAL, "value2", 256L));
        return new BRecordType("$$returnType$$", (Module) null, 0L, fields, (Type) null, true,
                IteratorUtils.getTypeFlags(PredefinedTypes.TYPE_DECIMAL));
    }

    public static RecordType getRecordStructRecord() {
        Map<String, Field> fields = new HashMap();
        fields.put("value0", new BField(getBooleanStructRecord(), "value0", 256L));
        return new BRecordType("$$returnType$$", (Module) null, 0L, fields, (Type) null, true,
                IteratorUtils.getTypeFlags(getBooleanStructRecord()));
    }

    public static Type getTupleType() {
        return new Type() {
            @Override
            public <V> V getZeroValue() {
                return null;
            }

            @Override
            public <V> V getEmptyValue() {
                return null;
            }

            @Override
            public int getTag() {
                return 34;
            }

            @Override
            public boolean isNilable() {
                return false;
            }

            @Override
            public String getName() {
                return "Utc";
            }

            @Override
            public String getQualifiedName() {
                return null;
            }

            @Override
            public Module getPackage() {
                return null;
            }

            @Override
            public boolean isPublic() {
                return false;
            }

            @Override
            public boolean isNative() {
                return false;
            }

            @Override
            public boolean isAnydata() {
                return false;
            }

            @Override
            public boolean isPureType() {
                return false;
            }

            @Override
            public boolean isReadOnly() {
                return false;
            }

            @Override
            public long getFlags() {
                return 0;
            }

            @Override
            public Type getImmutableType() {
                return null;
            }

            @Override
            public void setImmutableType(IntersectionType intersectionType) {

            }

            @Override
            public Module getPkg() {
                return null;
            }
        };
    }

    public static BTypedesc getBTypedesc(Type type) {
        Type typedesc = type;
        return new BTypedesc() {
            @Override
            public Type getDescribingType() {
                return typedesc;
            }

            @Override
            public Object instantiate(Strand strand) {
                return null;
            }

            @Override
            public Object instantiate(Strand strand, BInitialValueEntry[] bInitialValueEntries) {
                return null;
            }

            @Override
            public Object copy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public Object frozenCopy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public String stringValue(BLink bLink) {
                return null;
            }

            @Override
            public String expressionStringValue(BLink bLink) {
                return null;
            }

            @Override
            public Type getType() {
                return null;
            }
        };
    }

    public static <T> BArray getBArray(ArrayList<T> arrayList, Type type) {

        return new BArray() {
            @Override
            public int size() {
                return arrayList.size();
            }

            @Override
            public boolean isFrozen() {
                return BArray.super.isFrozen();
            }

            @Override
            public void freezeDirect() {
                BArray.super.freezeDirect();
            }

            @Override
            public void serialize(OutputStream outputStream) {
                BArray.super.serialize(outputStream);
            }

            @Override
            public BTypedesc getTypedesc() {
                return null;
            }

            @Override
            public Object get(long l) {
                return arrayList.get((int) l);
            }

            @Override
            public Object getRefValue(long l) {
                return null;
            }

            @Override
            public Object fillAndGetRefValue(long l) {
                return null;
            }

            @Override
            public long getInt(long l) {
                return 0;
            }

            @Override
            public boolean getBoolean(long l) {
                return false;
            }

            @Override
            public byte getByte(long l) {
                return 0;
            }

            @Override
            public double getFloat(long l) {
                return 0;
            }

            @Override
            public String getString(long l) {
                return null;
            }

            @Override
            public BString getBString(long l) {
                return null;
            }

            @Override
            public void add(long l, Object o) {

            }

            @Override
            public void add(long l, long l1) {

            }

            @Override
            public void add(long l, boolean b) {

            }

            @Override
            public void add(long l, byte b) {

            }

            @Override
            public void add(long l, double v) {

            }

            @Override
            public void add(long l, String s) {

            }

            @Override
            public void add(long l, BString bString) {

            }

            @Override
            public void append(Object o) {

            }

            @Override
            public Object reverse() {
                return null;
            }

            @Override
            public Object shift() {
                return null;
            }

            @Override
            public Object shift(long l) {
                return null;
            }

            @Override
            public void unshift(Object[] objects) {

            }

            @Override
            public Object[] getValues() {
                return new Object[0];
            }

            @Override
            public byte[] getBytes() {
                return new byte[0];
            }

            @Override
            public String[] getStringArray() {
                return new String[0];
            }

            @Override
            public long[] getIntArray() {
                return new long[0];
            }

            @Override
            public boolean[] getBooleanArray() {
                return new boolean[0];
            }

            @Override
            public byte[] getByteArray() {
                return new byte[0];
            }

            @Override
            public double[] getFloatArray() {
                return new double[0];
            }

            @Override
            public Type getElementType() {
                return null;
            }

            @Override
            public Type getIteratorNextReturnType() {
                return null;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public BArray slice(long l, long l1) {
                return null;
            }

            @Override
            public void setLength(long l) {

            }

            @Override
            public long getLength() {
                return 0;
            }

            @Override
            public BIterator<?> getIterator() {
                return null;
            }

            @Override
            public Object copy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public Object frozenCopy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public String stringValue(BLink bLink) {
                return null;
            }

            @Override
            public String informalStringValue(BLink parent) {
                return BArray.super.informalStringValue(parent);
            }

            @Override
            public String expressionStringValue(BLink bLink) {
                return null;
            }

            @Override
            public Type getType() {
                return type;
            }
        };
    }

    public static BMap getMockBMapRecord() {
        HashMap<BmpStringValue, Boolean> booleanHashMap = new HashMap<>();
        booleanHashMap.put(new BmpStringValue("value1"), true);
        booleanHashMap.put(new BmpStringValue("value2"), false);

        return new BMap() {
            @Override
            public Object get(Object o) {
                return booleanHashMap.get(o);
            }

            @Override
            public Object put(Object o, Object o2) {
                return null;
            }

            @Override
            public Object remove(Object o) {
                return null;
            }

            @Override
            public boolean containsKey(Object o) {
                return false;
            }

            @Override
            public Set<Map.Entry> entrySet() {
                return null;
            }

            @Override
            public Collection values() {
                return null;
            }

            @Override
            public void clear() {

            }

            @Override
            public Object getOrThrow(Object o) {
                return null;
            }

            @Override
            public Object fillAndGet(Object o) {
                return null;
            }

            @Override
            public Object[] getKeys() {
                return new Object[0];
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public void addNativeData(String s, Object o) {

            }

            @Override
            public Object getNativeData(String s) {
                return null;
            }

            @Override
            public Long getIntValue(BString bString) {
                return null;
            }

            @Override
            public Double getFloatValue(BString bString) {
                return null;
            }

            @Override
            public BString getStringValue(BString bString) {
                return null;
            }

            @Override
            public Boolean getBooleanValue(BString bString) {
                return null;
            }

            @Override
            public BMap<?, ?> getMapValue(BString bString) {
                return null;
            }

            @Override
            public BObject getObjectValue(BString bString) {
                return null;
            }

            @Override
            public BArray getArrayValue(BString bString) {
                return null;
            }

            @Override
            public Type getIteratorNextReturnType() {
                return null;
            }

            @Override
            public long getDefaultableIntValue(BString bString) {
                return 0;
            }

            @Override
            public Object merge(BMap bMap, boolean b) {
                return null;
            }

            @Override
            public BTypedesc getTypedesc() {
                return null;
            }

            @Override
            public void populateInitialValue(Object o, Object o2) {

            }

            @Override
            public BIterator<?> getIterator() {
                return null;
            }

            @Override
            public Object copy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public Object frozenCopy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public String stringValue(BLink bLink) {
                return null;
            }

            @Override
            public String expressionStringValue(BLink bLink) {
                return null;
            }

            @Override
            public Type getType() {
                return TestUtils.getBooleanStructRecord();
            }
        };
    }
}

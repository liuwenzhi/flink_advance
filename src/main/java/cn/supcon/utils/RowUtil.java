package cn.supcon.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.*;

public class RowUtil {

    public static String rowToJsonString(Row row){
        Map<String, Object> map = new HashMap<>();
        for (String fieldName : Objects.requireNonNull(row.getFieldNames(true))) {
            map.put(fieldName,row.getField(fieldName));
        }
        return new JSONObject(map).toString();
    }

    public static JSONObject rowToJsonObject(Row row){
        Map<String, Object> map = new HashMap<>();
        for (String fieldName : Objects.requireNonNull(row.getFieldNames(true))) {
            map.put(fieldName,row.getField(fieldName));
        }
        return new JSONObject(map);
    }

    public static  RowType getRowTypeByTable(Table table){
        List<RowType.RowField> rowFields = new ArrayList<>();
        for (Column column : table.getResolvedSchema().getColumns()) {
            String name = column.getName();
            LogicalType logicalType = column.getDataType().getLogicalType();
            rowFields.add(new RowType.RowField(name, logicalType));
        }
//        rowFields.add(new RowType.RowField("is_delete", new IntType()));
        return new RowType(rowFields);
    }

    protected static AbstractJdbcRowConverter.JdbcDeserializationConverter wrapIntoNullableInternalConverter(AbstractJdbcRowConverter.JdbcDeserializationConverter jdbcDeserializationConverter) {
        return (val) -> {
            return val == null ? null : jdbcDeserializationConverter.deserialize(val);
        };
    }

    protected static AbstractJdbcRowConverter.JdbcDeserializationConverter createNullableInternalConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createInternalConverter(type));
    }

    public static RowData toInternal(ResultSet resultSet, RowType rowType) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        AbstractJdbcRowConverter.JdbcDeserializationConverter[] jdbcDeserializationConverters = new AbstractJdbcRowConverter.JdbcDeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            jdbcDeserializationConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
        }
        for (int pos = 0; pos < rowType.getFieldCount(); ++pos) {
            Object field = resultSet.getObject(pos + 1);
            genericRowData.setField(pos, jdbcDeserializationConverters[pos].deserialize(field));
        }
        return genericRowData;
    }

    public static JSONObject resultSetToJson(ResultSet resultSet, RowType rowType) throws SQLException {
        JSONObject json = new JSONObject(rowType.getFieldCount());
        AbstractJdbcRowConverter.JdbcDeserializationConverter[] jdbcDeserializationConverters = new AbstractJdbcRowConverter.JdbcDeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            jdbcDeserializationConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
        }
        for (int pos = 0; pos < rowType.getFieldCount(); ++pos) {
            String fieldName = rowType.getFieldNames().get(pos);
            Object field = resultSet.getObject(pos + 1);
            json.put(fieldName, jdbcDeserializationConverters[pos].deserialize(field));
        }
        return json;
    }

    public static RowData toInternal(JSONObject json, RowType rowType) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        AbstractJdbcRowConverter.JdbcDeserializationConverter[] jdbcDeserializationConverters = new AbstractJdbcRowConverter.JdbcDeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            jdbcDeserializationConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
        }
        for (int pos = 0; pos < rowType.getFieldCount(); ++pos) {
            String fieldName = rowType.getFieldNames().get(pos);
            Object fieldValue = json.get(fieldName);
            if(fieldValue instanceof String){
                fieldValue  = StringData.fromString((String) fieldValue);
            }
            genericRowData.setField(pos, fieldValue);
        }
        return genericRowData;
    }

    public static Row toInternalRow(JSONObject json, RowType rowType) throws SQLException {
        Row row = new Row(rowType.getFieldCount());
        AbstractJdbcRowConverter.JdbcDeserializationConverter[] jdbcDeserializationConverters = new AbstractJdbcRowConverter.JdbcDeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            jdbcDeserializationConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
        }
        for (int pos = 0; pos < rowType.getFieldCount(); ++pos) {
            String fieldName = rowType.getFieldNames().get(pos);
            Object fieldValue = json.get(fieldName);
            if(fieldValue instanceof String){
                fieldValue  = StringData.fromString((String) fieldValue);
            }
            row.setField(pos, fieldValue);
        }
        return row;
    }

    private static  AbstractJdbcRowConverter.JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (val) -> {
                    return null;
                };
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return (val) -> {
                    return val;
                };
            case TINYINT:
                return (val) -> {
                    return ((Integer) val).byteValue();
                };
            case SMALLINT:
                return (val) -> {
                    return val instanceof Integer ? ((Integer) val).shortValue() : val;
                };
            case INTEGER:
                return (val) -> {
                    return val;
                };
            case BIGINT:
                return (val) -> {
                    return val;
                };
            case DECIMAL:
                int precision = ((DecimalType) type).getPrecision();
                int scale = ((DecimalType) type).getScale();
                return (val) -> {
                    return val instanceof BigInteger ? DecimalData.fromBigDecimal(new BigDecimal((BigInteger) val, 0), precision, scale) : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
                };
            case DATE:
                return (val) -> {
                    return (int) ((Date) val).toLocalDate().toEpochDay();
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (val) -> {
                    return (int) (((Time) val).toLocalTime().toNanoOfDay() / 1000000L);
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val) -> {
                    return val instanceof LocalDateTime ? TimestampData.fromLocalDateTime((LocalDateTime) val) : TimestampData.fromTimestamp((Timestamp) val);
                };
            case CHAR:
            case VARCHAR:
                return (val) -> {
//                    return StringData.fromString((String) val);
                    return val;
                };
            case BINARY:
            case VARBINARY:
                return (val) -> {
                    return val;
                };
            case ARRAY:
            case ROW:
            case MAP:
                return (val) -> {
//                    return StringData.fromString((String) val);
                    return val;
                };
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

}

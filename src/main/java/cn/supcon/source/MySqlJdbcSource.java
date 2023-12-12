package cn.supcon.source;

import cn.supcon.utils.RowUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 自定义 mysql数据库jdbcSource源
 */
public class MySqlJdbcSource extends RichSourceFunction<JSONObject> implements CheckpointedFunction {

    private PreparedStatement preparedStatement = null;

    private Connection connection = null;

    String offsetColumn = "id";

    String url = "jdbc:mysql://172.16.2.23:3306";

    String username = "root";

    String password = "Supcon_21";

    String database = "data-operation-test";

    String table = "event";

    String fieldToType = "id:int,name:String,url:String,money:bigint";

    Map<String, String> fieldToTypeMap = Arrays.stream(fieldToType.split(",")).map(item -> item.split(":")).collect(Collectors.toMap(item -> item[0], item -> item[1]));

    ListState<Object> checkpointState;

    Object offsetValue;

    public Connection getConnection(String url, String username, String password) {
        try {
            connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public MySqlJdbcSource() {
        super();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointState.clear();
        checkpointState.update(Collections.singletonList(offsetValue));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Object> descriptor = new ListStateDescriptor<>("offset", TypeInformation.of(new TypeHint<Object>() {
        }));
        // 初始化checkpoint
        checkpointState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            // 从checkpoint获取当前offset
            for (Object element : checkpointState.get()) {
                offsetValue = element;
            }
        } else {
            // 原始表采用自增主键，最小值是1
            offsetValue = 1;
        }

    }

    @Override
    public void run(SourceContext<JSONObject> sourceContext) throws Exception {
        // 持续读取数据
        while (true) {
            String sql = "select id,name,url,money from `data-operation-test`.`event` where id >= " + offsetValue;
            preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                offsetValue = resultSet.getInt("id");
                JSONObject jsonObject = RowUtil.resultSetToJson(resultSet,getRowType());
                sourceContext.collect(jsonObject);
            }
            /*ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                offsetValue = resultSet.getInt("id");
                JSONObject jsonObject = new JSONObject();
                for (int i = 0; i < columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String value = resultSet.getString(columnName);
                    jsonObject.put(columnName, value);
                }
                // 数据扔给上下文下游算子
                sourceContext.collect(jsonObject);
            }*/
            TimeUnit.SECONDS.sleep(5);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection(url, username, password);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

    @Override
    public void cancel() {

    }

    /**
     * 返回行字段类型
     */
    public RowType getRowType() {
        RowType rowType = new RowType((Arrays.asList(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("url", new VarCharType()),
                new RowType.RowField("money", new BigIntType()))));
        return rowType;
    }
}

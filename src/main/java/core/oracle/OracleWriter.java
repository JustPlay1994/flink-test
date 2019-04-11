package core.oracle;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Description mysql sink
 * @Author jiangxiaozhi
 * @Date 2018/10/15 18:31
 **/
public class OracleWriter extends RichSinkFunction<Tuple2<Double,Double>> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    String username = "TEST";
    String password = "123456";
    String url = "jdbc:oracle:thin:@10.217.17.71:1521/orcl";
    String driver = "oracle.jdbc.OracleDriver";
    String sql = "insert into BLK_SANXIAO_PLACE(jd84, wd84, object_id) VALUES(?,?,?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(driver);
        // 获取数据库连接
        connection = DriverManager.getConnection(url, username, password);//写入mysql数据库
        preparedStatement = connection.prepareStatement(sql);//insert sql在配置文件中
        super.open(parameters);
    }
 
    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
 
    @Override
    public void invoke(Tuple2<Double,Double> value, Context context) throws Exception {
        try {
            Double account = value.getField(0);
            Double password = value.getField(1);
            preparedStatement.setDouble(1,account);
            preparedStatement.setDouble(2,password);
            preparedStatement.setInt(3,IdGenerator.getId());
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
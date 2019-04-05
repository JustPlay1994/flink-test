package core.mysql;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
 
/**
 * @Description mysql sink
 * @Author jiangxiaozhi
 * @Date 2018/10/15 18:31
 **/
public class JdbcWriter extends RichSinkFunction<Tuple2<String,String>> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    String username = "root";
    String password = "123456";
    String url = "jdbc:mysql://localhost:3306/test?autoReconnect=true&useSSL=false&user="+username+"&password="+password;
    String driver = "com.mysql.jdbc.Driver";
    String sql = "insert into user1(account, password) VALUES(?,?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(driver);
        // 获取数据库连接
        connection = DriverManager.getConnection(url);//写入mysql数据库
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
    public void invoke(Tuple2<String,String> value, Context context) throws Exception {
        try {
            String account = value.getField(0);
            String password = value.getField(1);
            preparedStatement.setString(1,account);
            preparedStatement.setString(2,password);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
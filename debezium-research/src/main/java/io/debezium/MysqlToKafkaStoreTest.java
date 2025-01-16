package io.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * description: 监听mysql数据变化并记录到kafka中
 * company: 北京海量数据有限公司
 * create by: zhaosong 2025/1/16
 * version: 1.0
 */
public class MysqlToKafkaStoreTest {

    private final static String DB_HOST;

    private final static String DB_PWD;

    private final static String KAFKA_SERVERS;


    private static DebeziumEngine<ChangeEvent<String, String>> engine;

    static {
        String osType = System.getProperty("os.name");
        if (StringUtils.contains(osType, "window")) {
            DB_HOST = "192.168.1.53";
            DB_PWD = "Vbase@1234";
            KAFKA_SERVERS = "192.168.1.53:9092";
        } else {
            DB_HOST = "10.211.55.20";
            DB_PWD = "root@123";
            KAFKA_SERVERS = "10.211.55.20:9092";
        }
    }

    public static void main(String[] args) {
        // 1.配置Debezium Engine
        // 参考：https://debezium.io/documentation/reference/1.9/development/engine.html
        Properties props = new Properties();
        props.setProperty("name", "mysql-connector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.KafkaOffsetBackingStore"); //负责连接器偏移量持久性的Java类的名称
        props.setProperty("bootstrap.servers", KAFKA_SERVERS);
        props.setProperty("offset.storage.topic", "mysql_cdc_offsets"); //存储偏移量的Kafka主题的名称
        props.setProperty("offset.storage.partitions", "1"); //创建偏移量存储主题时使用的分区数
        props.setProperty("offset.storage.replication.factor", "1"); //创建偏移量存储主题时使用的复制因子
        // 提交策略的Java类的名称。它根据处理的事件数和自上次提交以来经过的时间来定义何时触发偏移提交。此类必须实现接口<…​>.OffsetCommitPolicy。默认值为基于时间间隔的定期提交策略。
//        props.setProperty("offset.commit.policy", "io.debezium.engine.spi.OffsetCommitPolicy.PeriodicCommitOffsetPolicy");
        // 尝试提交偏移量的时间间隔。默认值为1分钟。
//        props.setProperty("offset.flush.interval.ms", "60000");
        // 在取消进程并恢复要在以后尝试中提交的偏移数据之前，等待记录刷新并将分区数据提交给偏移存储的最大毫秒数。默认值为5秒。
//        props.setProperty("offset.flush.timeout.ms", "5000");
        // 应该用于序列化和反序列化偏移量的关键数据的Converter类。默认值为JSON转换器。
//        props.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        // 应该用于对偏移量的值数据进行序列化和反序列化的Converter类。默认值为JSON转换器。
//        props.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");

        // 2.mysql connector的参数配置
        // 参考：https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-connector-properties
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");// 连接器的 Java 类的名称。
        props.setProperty("database.hostname", DB_HOST);
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "root");
        props.setProperty("database.password", DB_PWD);
        props.setProperty("database.server.id", "184054");
        // 标识并为 Debezium 捕获更改的特定 MySQL 数据库服务器/集群提供名称空间的逻辑名称。逻辑名称在所有其他连接器中应该是唯一的，
        // 因为它用作接收此连接器发出的事件的所有 Kafka 主题名称的前缀。数据库服务器逻辑名称中只能使用字母数字字符、连字符、点和下划线。
        props.setProperty("database.server.name", "mysql-kafka-connector");
        props.setProperty("database.include.list", "test");
        // 指定连接器启动时运行快照的条件
        props.setProperty("snapshot.mode", "schema_only");
        // 负责数据库历史记录持久性的 Java 类的名称
        //它必须实现 <...> . DatabaseHistory 接口。
        props.setProperty("database.history", "io.debezium.relational.history.KafkaDatabaseHistory");
        // 要连接到的 Kafka 集群服务器的初始列表。集群提供了存储数据库历史记录的主题。
        // 当 datase.history 设置为 <...> . KafkaDatabaseHistory 时必需。
        props.setProperty("database.history.kafka.bootstrap.servers", KAFKA_SERVERS);
        props.setProperty("database.history.kafka.topic", "mysql_cdc_history");
//        props.setProperty("include.schema.changes", "true");

        // 使用上述配置创建Debezium引擎，输出样式为Json字符串格式
        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    System.out.println(record.value());//输出到控制台
                })
                .using((success, message, error) -> {
                    if (error != null) {
                        error.printStackTrace();
                        // 报错回调
                        System.out.println("------------error, message:" + message);
                        System.out.println("exception:" + error);
                    }
                    closeEngine(engine);
                })
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        addShutdownHook(engine);
        awaitTermination(executor);
        System.out.println("------------main finished.");
    }

    private static void closeEngine(DebeziumEngine<ChangeEvent<String, String>> engine) {
        try {
            engine.close();
        } catch (IOException ignored) {

        }
    }

    private static void addShutdownHook(DebeziumEngine<ChangeEvent<String, String>> engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeEngine(engine)));
    }

    private static void awaitTermination(ExecutorService executor) {
        if (executor != null) {
            try {
                executor.shutdown();
                while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {

                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

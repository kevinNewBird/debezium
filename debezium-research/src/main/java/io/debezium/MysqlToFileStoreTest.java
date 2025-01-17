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
 * description: 监听mysql数据变化并记录到本地文件中
 * company: 北京海量数据有限公司
 * create by: zhaosong 2025/1/16
 * version: 1.0
 */
public class MysqlToFileStoreTest {

    private final static String DB_HOST;

    private final static String DB_PWD;


    private static DebeziumEngine<ChangeEvent<String, String>> engine;

    static {
        String osType = System.getProperty("os.name");
        if (StringUtils.containsIgnoreCase(osType, "window")) {
            DB_HOST = "192.168.1.53";
            DB_PWD = "Vbase@1234";
        } else {
            DB_HOST = "10.211.55.20";
            DB_PWD = "root@123";
        }
    }

    public static void main(String[] args) throws Exception {

        final Properties props = new Properties();
        // 1.engine的参数设置
        props.setProperty("name", "dbz-engine");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        // 使用文件来存储已处理的binlog偏移量
        props.setProperty("offset.storage.file.filename", "/Users/zhaosong/workspace/logs/mysql_offsets.log");
        props.setProperty("offset.flush.interval.ms", "6000");
        props.setProperty("converter.schemas.enable", "true");

        // 2.mysql connector的参数配置
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("database.hostname", DB_HOST);
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "root");
        props.setProperty("database.password", DB_PWD);
        props.setProperty("database.server.id", "122110"); //随机设置
        props.setProperty("database.server.name", "mysql-connector");
        props.setProperty("database.include.list", "test");//要捕获的数据库名
        props.setProperty("snapshot.mode", "schema_only");//全量+增量
        props.setProperty("decimal.handling.mode", "double");
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename", "/Users/zhaosong/workspace/logs/mysql_dbhistory.log");

        // 使用上述配置创建Debezium引擎，输出样式为Json字符串格式
        engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    //test.sendMsg(record.value());
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

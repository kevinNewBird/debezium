package io.debezium;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * description: 监听mysql数据变化并记录到本地文件中，实现变更记录的自实现ChangeConsumer
 * company: 北京海量数据有限公司
 * create by: zhaosong 2025/1/16
 * version: 1.0
 */
public class OracleToFileStoreConsumerTest {

    private static Logger log = Logger.getLogger(OracleToFileStoreConsumerTest.class);

    private final static String DB_HOST;

    private final static String DB_PWD;

    private final static String STORAGE_FILE;

    private final static String HISTORY_FILE;

    private static EmbeddedEngine engine;

    private static final MysqlSourceChangeConsumer sourceChangeConsumer = new MysqlSourceChangeConsumer();

    static {
        String osType = System.getProperty("os.name");
        if (StringUtils.containsIgnoreCase(osType, "window")) {
            DB_HOST = "192.168.1.53";
            DB_PWD = "Vbase@1234";
            STORAGE_FILE = "D:/tmp/dbz/storage/oracle_offsets.log";
            HISTORY_FILE = "D:/tmp/dbz/storage/oracle_dbhistory.log";
        } else {
            DB_HOST = "192.168.231.155";
            DB_PWD = "123456";
            STORAGE_FILE = "/Users/zhaosong/workspace/logs/oracle_offsets.log";
            HISTORY_FILE = "/Users/zhaosong/workspace/logs/oracle_dbhistory.log";
        }

        PropertyConfigurator.configure(OracleToFileStoreConsumerTest.class.getClassLoader().getResource("log4j.properties"));
    }

    /*
     Debezium中的SourceRecord和SinkRecord是数据同步过程中的关键概念。‌
     1.SourceRecord:
         SourceRecord‌是Debezium在捕获数据库变更时生成的数据记录。它包含了变更事件的详细信息，如数据库表名、变更类型（插入、更新、删除）、
     变更前后的数据等。SourceRecord的主要作用是将数据库的变更事件以事件流的形式传递给消费者，使得应用程序可以及时响应数据库的变化‌
     2.SinkRecord
         SinkRecord‌是Debezium在将数据同步到目标数据库或存储系统时生成的数据记录。它包含了从SourceRecord转换后的数据，以及目标系统的相
     关信息。SinkRecord确保数据能够准确地写入目标系统，保持数据的一致性和完整性‌
    */
    public static void main(String[] args) throws Exception {

        final Properties props = new Properties();
        // 1.engine的参数设置
        props.setProperty("name", "dbz-engine");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        // 使用文件来存储已处理的binlog偏移量
        props.setProperty("offset.storage.file.filename", STORAGE_FILE);
        props.setProperty("offset.flush.interval.ms", "6000");
        props.setProperty("converter.schemas.enable", "true");

        // 2.mysql connector的参数配置
        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
        props.setProperty("database.hostname", DB_HOST);
        props.setProperty("database.port", "1521");
        props.setProperty("database.user", "system");
        props.setProperty("database.password", DB_PWD);
        props.setProperty("database.dbname", "ORCL");//要捕获的数据库名
        props.setProperty("database.server.name", "oracle-connector");
        props.setProperty("topic.prefix", "oracle231_150");
        props.setProperty("tasks.max","1");
        props.setProperty("snapshot.mode", "schema_only");//全量+增量
        props.setProperty("schema.include.list", "testdb");
//        props.setProperty("schema.history.internal.kafka.bootstrap.servers","kafka:9092");
//        props.setProperty("schema.history.internal.kafka.topic","schema-changes.inventory");
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename", HISTORY_FILE);

        // 使用上述配置创建Debezium引擎，输出样式为Json字符串格式
        engine = new EmbeddedEngine.BuilderImpl()
                .using(props)
                // 注意：自实现consumer并不会影响offset.storage和database.history的工作
                // 可通过查看mysql_offsets.log的更新时间和文件内容进行佐证
                .notifying(sourceChangeConsumer)
                .using(OracleToFileStoreConsumerTest.class.getClassLoader())
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        addShutdownHook(engine);
        awaitTermination(executor);
        System.out.println("------------main finished.");
    }

    private static void closeEngine(DebeziumEngine<SourceRecord> engine) {
        try {
            engine.close();
        } catch (IOException ignored) {

        }
    }

    private static void addShutdownHook(DebeziumEngine<SourceRecord> engine) {
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

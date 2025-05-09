package io.test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public class PostgresTableLockTest {

    private static final long LOCK_TIMEOUT_SECONDS = TimeUnit.SECONDS.toMillis(10);
    private static final String TX_ISOLATION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;";
    private static final String LOCK_TIMEOUT_SQL = "set lock_timeout = " + LOCK_TIMEOUT_SECONDS;
    private static final String LOCK_TEMPLATE = "LOCK TABLE %s IN ACCESS SHARE MODE";
    private static final List<String> SCHEMA_LIST;
    private static final String SEMICOLON_CONS = ";";
    private static final String LINE_SEPARATOR = System.lineSeparator();

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        List<String> schemaList = new ArrayList<String>(100);
        String SCHEMA_TEMPLATE = "test.table_%s";
        for (int i = 1; i <= 10; i++) {
            schemaList.add(String.format(SCHEMA_TEMPLATE, i + ""));
        }

//        SCHEMA_TEMPLATE = "test.tb_%s";
//        for (int i = 1; i <= 20000; i++) {
//            schemaList.add(String.format(SCHEMA_TEMPLATE, i + ""));
//        }
        SCHEMA_LIST = schemaList;
    }

    public static void main(String[] args) throws SQLException {
        testLock(1);
    }

    private static void testLock(int loop) {
        long totalTime = 0;
        for (int i = 0; i < loop; i++) {
            String url = "jdbc:postgresql://192.168.231.150:5432/testdb";
            String username = "test";
            String password = "root@123";
            long startTime = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(url, username, password);) {
                conn.setAutoCommit(false);
                // 生成执行sql
//                Optional<String> optional = generateLockSql();
                Optional<String> optional = generateTxSql();
                if (!optional.isPresent()) {
                    return;
                }
                try (Statement stmt = conn.createStatement();) {
                    stmt.execute(optional.get());
                }
                DatabaseMetaData metaData = conn.getMetaData();
                try (ResultSet rs = metaData.getTables("testdb", "test", null, new String[]{"VIEW", "MATERIALIZED VIEW", "TABLE", "PARTITIONED TABLE"});) {
                    int index = 0;
                    while (rs.next()) {
                        String catalogName = rs.getString(1);
                        String schemaName = rs.getString(2);
                        String tableName = rs.getString(3);
                        String tableType = rs.getString(4);
                        if ("TABLE".equals(tableType) || "PARTITIONED TABLE".equals(tableType)) {
                            System.out.printf("[%s] catalog: %s, schema:%s, table:%s, type:%s%n"
                                    , ++index, catalogName, schemaName, tableName, tableType);
                        }
                    }
                }
                conn.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            long endTime = System.currentTimeMillis();
            System.out.printf("【x】[%s] 耗时：%sms%n", i + "", (endTime - startTime));
            totalTime += (endTime - startTime);
        }

        System.out.printf("【x】%s次平均耗时：%sms%n", loop + "", totalTime / loop);
    }

    private static Optional<String> generateTxSql() {
        return Optional.of(TX_ISOLATION_SQL);
    }

    private static Optional<String> generateLockSql() {
        StringBuilder builder = new StringBuilder();
        builder.append(LOCK_TIMEOUT_SQL).append(SEMICOLON_CONS);
        SCHEMA_LIST.forEach(tableId ->
                builder.append(String.format(LOCK_TEMPLATE, tableId)).append(SEMICOLON_CONS).append(LINE_SEPARATOR));
        return Optional.of(builder.toString());
    }
}

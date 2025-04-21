package io.test;

import java.sql.*;
import java.lang.String;
import java.util.ArrayList;
import java.util.List;

public class OracleTableLockTest {


    private static final String LOCK_TEMPLATE = "LOCK TABLE %s IN ROW SHARE MODE";
    private static final List<String> SCHEMA_LIST;

    static {
        try {
            // 动态加载Oracle驱动类‌:ml-citation{ref="1,2" data="citationList"}
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        List<String> schemaList = new ArrayList<String>(100);
        String SCHEMA_TEMPLATE = "TESTDB.TB%s";
        for (int i = 1; i <= 190; i++) {
            schemaList.add(String.format(SCHEMA_TEMPLATE, i + ""));
        }
//        SCHEMA_TEMPLATE = "TESTDB.TABLE_%s";
//        for (int i = 1; i <= 20000; i++) {
//            schemaList.add(String.format(SCHEMA_TEMPLATE, i + ""));
//        }
        SCHEMA_LIST = schemaList;
    }

    public static void main(String[] args) throws SQLException {
//        testLockCommon(5);
        testLockBatch(1);
    }

    private static void testLockBatch(int loop) {
        long totalTime = 0;
        for (int i = 0; i < loop; i++) {
            String url = "jdbc:oracle:thin:@192.168.231.155:1521:orcl";
            String username = "system";
            String password = "123456";
            long startTime = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(url, username, password);) {
                conn.setAutoCommit(false);
//                long startTime1 = System.currentTimeMillis();
                Savepoint savepoint1 = conn.setSavepoint("savepoint1");
                try (Statement stmt = conn.createStatement();) {
                    int batchSize = 500;
                    int batchCount = 0;
                    for (String tableId : SCHEMA_LIST) {
                        stmt.addBatch(String.format(LOCK_TEMPLATE, tableId));
                        batchCount++;
                        if (batchCount == batchSize) {
//                            long startTime2 = System.currentTimeMillis();
                            stmt.executeBatch();
                            stmt.clearBatch();
//                            long endTime2 = System.currentTimeMillis();
//                            System.out.printf("【x-batch】[%s] 耗时：%sms%n", i + "", (endTime2 - startTime2));
                            batchCount = 0;
                        }
                    }
                    if (SCHEMA_LIST.size() % batchSize != 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }

                }
//                long endTime1 = System.currentTimeMillis();
//                System.out.printf("【x1】[%s] 耗时：%sms%n", i + "", (endTime1 - startTime1));
                conn.rollback(savepoint1);
//                long endTime2 = System.currentTimeMillis();
//                System.out.printf("【x2】[%s] 耗时：%sms%n", i + "", (endTime2 - endTime1));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            long endTime = System.currentTimeMillis();
            System.out.printf("【x】[%s] 耗时：%sms%n", i + "", (endTime - startTime));
            totalTime += (endTime - startTime);
        }

        System.out.printf("【x】%s次平均耗时：%sms%n", loop + "", totalTime / loop);
    }

    private static void testLockCommon(int loop) {
        long totalTime = 0;
        for (int i = 0; i < loop; i++) {
            String url = "jdbc:oracle:thin:@192.168.231.155:1521:orcl";
            String username = "system";
            String password = "123456";
            long startTime = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(url, username, password);) {
                conn.setAutoCommit(false);
                Savepoint savepoint2 = conn.setSavepoint("savepoint2");
                try (Statement stmt = conn.createStatement();) {
                    for (String tableId : SCHEMA_LIST) {
                        stmt.execute(String.format(LOCK_TEMPLATE, tableId));
                    }
                }
                conn.rollback(savepoint2);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            long endTime = System.currentTimeMillis();
            System.out.printf("【y】[%s] 耗时：%sms%n", i + "", (endTime - startTime));
            totalTime += (endTime - startTime);
        }

        System.out.printf("【y】%s次平均耗时：%sms%n", loop + "", totalTime / loop);
    }
}

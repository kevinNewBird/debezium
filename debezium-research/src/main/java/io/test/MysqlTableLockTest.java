package io.test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class MysqlTableLockTest {

    private static final String LOCK_TEMPLATE = "FLUSH TABLES %s WITH READ LOCK";
    private static final List<String> SCHEMA_LIST;

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        List<String> schemaList = new ArrayList<String>(100);
        String SCHEMA_TEMPLATE = "table_%s";
//        for (int i = 1; i <= 100; i++) {
//            schemaList.add(String.format(SCHEMA_TEMPLATE, i + ""));
//        }

        SCHEMA_TEMPLATE = "tb_%s";
        for (int i = 1; i <= 1000; i++) {
            schemaList.add(String.format(SCHEMA_TEMPLATE, i + ""));
        }
        SCHEMA_LIST = schemaList;
    }

    public static void main(String[] args) throws SQLException {
        testLockCommon(10);
    }

    private static void testLockCommon(int loop) {
        long totalTime = 0;
        for (int i = 0; i < loop; i++) {
            String url = "jdbc:mysql://192.168.231.150:3306/testdb?characterEncoding=utf8&useSSL=false";
            String username = "root";
            String password = "root@123";
            long startTime = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(url, username, password);) {
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement();) {
                    StringJoiner joiner = new StringJoiner(",");
                    for (String tableId : SCHEMA_LIST) {
                        joiner.add(tableId);
                    }
                    stmt.execute(String.format(LOCK_TEMPLATE, joiner.toString()));
                    stmt.execute("UNLOCK TABLES");
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            long endTime = System.currentTimeMillis();
            System.out.printf("【x】[%s] 耗时：%sms%n", i + "", (endTime - startTime));
            totalTime += (endTime - startTime);
        }

        System.out.printf("【x】%s次平均耗时：%sms%n", loop + "", totalTime / loop);
    }
}

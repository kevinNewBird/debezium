package io.debezium;

import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * description: io.debezium
 * company: 北京海量数据有限公司
 * create by: zhaosong 2025/1/17
 * version: 1.0
 */
public class MysqlSourceChangeConsumer implements DebeziumEngine.ChangeConsumer<SourceRecord> {


    /*
      Debezium中的SourceRecord和SinkRecord是数据同步过程中的关键概念。‌
      1.SourceRecord:
          SourceRecord‌是Debezium在捕获数据库变更时生成的数据记录。它包含了变更事件的详细信息，如数据库表名、变更类型（插入、更新、删除）、
      变更前后的数据等。SourceRecord的主要作用是将数据库的变更事件以事件流的形式传递给消费者，使得应用程序可以及时响应数据库的变化‌
      2.SinkRecord
          SinkRecord‌是Debezium在将数据同步到目标数据库或存储系统时生成的数据记录。它包含了从SourceRecord转换后的数据，以及目标系统的相
     关信息。SinkRecord确保数据能够准确地写入目标系统，保持数据的一致性和完整性‌
     */
    @Override
    public void handleBatch(List<SourceRecord> records
            , DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
        // 判断数据是否为空
        if (records.isEmpty()) {
            return;
        }

        for (SourceRecord record : records) {
            System.out.println(record);
        }

        SourceRecord offsetEnd = records.get(records.size() - 1);
        // committer中有两个重要方法: 结合使用
        committer.markProcessed(offsetEnd); // 标记结束的offset, 即最后一条变更记录
        committer.markBatchFinished(); // 标记批次变更数据结束
    }
}

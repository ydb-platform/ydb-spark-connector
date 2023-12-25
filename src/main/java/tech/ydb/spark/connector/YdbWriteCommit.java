package tech.ydb.spark.connector;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * YDB writer commit message.
 *
 * @author zinal
 */
public class YdbWriteCommit implements WriterCommitMessage {

    // currently empty - no need to transfer anything

}

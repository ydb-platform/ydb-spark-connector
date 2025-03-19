package tech.ydb.spark.connector.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * YDB writer commit message.
 *
 * @author zinal
 */
public class YdbWriteCommit implements WriterCommitMessage {

    private static final long serialVersionUID = 1L;

    // currently empty - no need to transfer anything

}

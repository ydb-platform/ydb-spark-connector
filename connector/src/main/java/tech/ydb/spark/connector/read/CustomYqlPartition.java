package tech.ydb.spark.connector.read;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class CustomYqlPartition implements InputPartition {
    private static final long serialVersionUID = 5327901814847550038L;

    public static final InputPartition INSTANCE = new CustomYqlPartition();
    public static final InputPartition[] PLAN = new InputPartition[] {INSTANCE};

    private CustomYqlPartition() { }
}

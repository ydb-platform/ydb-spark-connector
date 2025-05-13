package tech.ydb.spark.connector.read;

import org.apache.spark.sql.connector.read.InputPartition;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class TabletPartition implements InputPartition {
    private static final long serialVersionUID = -4836481111366035711L;

    private final int tablet;

    public TabletPartition(int tablet) {
        this.tablet = tablet;
    }

    public int getTablet() {
        return tablet;
    }

    @Override
    public String toString() {
        return (tablet <= 0) ? "tablet:unconfined" : ("tablet:" + tablet);
    }

}

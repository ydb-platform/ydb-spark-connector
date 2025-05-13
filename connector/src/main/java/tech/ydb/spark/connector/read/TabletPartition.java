package tech.ydb.spark.connector.read;

import org.apache.spark.sql.connector.read.InputPartition;


/**
 *
 * @author Aleksandr Gorshenin
 */
public class TabletPartition implements InputPartition {
    private static final long serialVersionUID = -4836481111366035711L;

    private final String tabletId;

    public TabletPartition(String tabletId) {
        this.tabletId = tabletId;
    }

    public String getTabletId() {
        return tabletId;
    }

    @Override
    public String toString() {
        return (tabletId == null) ? "tablet:unconfined" : ("tablet:" + tabletId);
    }

}

package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

import tech.ydb.table.result.ResultSetReader;

/**
 * YDB table or index scan implementation abstract logic.
 *
 * @author zinal
 */
public abstract class ReadAbstract implements AutoCloseable {

    private static final org.slf4j.Logger LOG
            = org.slf4j.LoggerFactory.getLogger(ReadAbstract.class);

    protected final YdbScanOptions options;
    protected final YdbKeyRange keyRange;
    protected final String tablePath;
    protected final List<String> outColumns;
    private int[] outIndexes;
    private volatile State state;
    private volatile Exception firstIssue;
    private ResultSetReader current;

    ReadAbstract(YdbScanOptions options, YdbKeyRange keyRange) {
        this.options = options;
        this.keyRange = keyRange;
        this.tablePath = options.getTablePath();
        this.outColumns = new ArrayList<>();
        this.state = State.CREATED;
    }

    public void prepare() {
        if (getState() != State.CREATED) {
            return;
        }

        LOG.debug("Configuring scan for table {}, range {}, columns {}, types {}",
                tablePath, keyRange, options.getKeyColumns(), options.getKeyTypes());

        // Add all required fields.
        outColumns.clear();
        scala.collection.Iterator<StructField> sfit = options.readSchema().seq().iterator();
        while (sfit.hasNext()) {
            String colname = sfit.next().name();
            outColumns.add(colname);
        }
        if (outColumns.isEmpty()) {
            // In case no fields are required, add the first field of the primary key.
            String colname = options.getKeyColumns().get(0);
            outColumns.add(colname);
        }
        outIndexes = new int[outColumns.size()];

        // Create or acquire the connector object.
        YdbConnector yc = YdbRegistry.getOrCreate(options.getCatalogName(), options.getConnectOptions());
        try {
            // Delegate the preparation to the overridden method.
            prepareScan(yc);
        } catch (Exception ex) {
            setIssue(ex);
            LOG.warn("Failed to initiate scan for table {}", tablePath, ex);
            throw new RuntimeException("Failed to initiate scan for table " + tablePath, ex);
        }

        current = null;
        state = State.PREPARED;
    }

    public boolean next() {
        // If we have a row block, return its rows before checking any state.
        if (current != null && current.next()) {
            return true; // we have the current row
        }        // no block, or end of rows in the block
        switch (getState()) {
            case PREPARED:
                return doNext();
            case FAILED:
                throw new RuntimeException("Scan failed.", getIssue());
            case CREATED:
                throw new IllegalStateException("Scan has not been prepared.");
            default:
                return false;
        }
    }

    private boolean doNext() {
        while (true) {
            if (current != null && current.next()) {
                return true; // have next row in the current block
            }            // end of rows or no block - need next
            current = nextScan();
            final Exception issue = getIssue();
            if (issue != null) {
                current = null;
                setState(State.FAILED);
                throw new RuntimeException("Scan failed", issue);
            }
            if (current == null) {
                setState(State.FINISHED);
                LOG.debug("No more blocks for table {}", tablePath);
                return false;
            }
            // Rebuild column indexes each block, because API allows
            // the server to change the column ordering.
            if (outIndexes.length != current.getColumnCount()) {
                throw new RuntimeException("Expected columns count "
                        + outIndexes.length + ", but got " + current.getColumnCount());
            }
            for (int i = 0; i < outIndexes.length; ++i) {
                outIndexes[i] = current.getColumnIndex(outColumns.get(i));
                if (outIndexes[i] < 0) {
                    throw new RuntimeException("Lost column [" + outColumns.get(i)
                            + "] in the result set");
                }
            }
            LOG.debug("Fetched the block of {} rows from the queue for table {}",
                    current.getRowCount(), tablePath);
        }
    }

    public InternalRow get() {
        final int count = outIndexes.length;
        final ArrayList<Object> values = new ArrayList<>(count);
        for (int i = 0; i < count; ++i) {
            values.add(options.getTypes().convertFromYdb(current.getColumn(outIndexes[i])));
        }
        return InternalRow.fromSeq(JavaConversions.asScalaBuffer(values));
    }

    @Override
    public void close() {
        setState(State.FINISHED);
        closeScan();
        current = null;
    }

    protected synchronized void setIssue(Exception issue) {
        if (firstIssue == null) {
            firstIssue = issue;
            state = State.FAILED;
        }
    }

    private synchronized Exception getIssue() {
        return firstIssue;
    }

    private synchronized State getState() {
        return state;
    }

    private synchronized void setState(State state) {
        this.state = state;
    }

    protected abstract void prepareScan(YdbConnector yc);
    protected abstract void closeScan();
    protected abstract ResultSetReader nextScan();

    enum State {
        CREATED,
        PREPARED,
        FINISHED,
        FAILED
    }

}

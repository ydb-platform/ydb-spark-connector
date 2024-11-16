package tech.ydb.spark.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.JavaConverters;

/**
 *
 * @author zinal
 */
public class YdbWriteBuilder implements WriteBuilder, SupportsTruncate {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbWriteBuilder.class);

    private final YdbTable table;
    private final LogicalWriteInfo info;
    private final boolean truncate;

    public YdbWriteBuilder(YdbTable table, LogicalWriteInfo info, boolean truncate) {
        this.table = table;
        this.info = info;
        this.truncate = truncate;
    }

    @Override
    public Write build() {
        LOG.debug("Creating YdbWrite for table {}", table.getTablePath());
        boolean mapByNames = validateSchemas(table.schema(), info.schema());
        return new YdbWrite(table, info, mapByNames, truncate);
    }

    @Override
    public WriteBuilder truncate() {
        LOG.debug("Truncation requested for table {}", table.getTablePath());
        return new YdbWriteBuilder(table, info, true);
    }

    private boolean validateSchemas(StructType actualSchema, StructType inputSchema) {
        final List<StructField> inputFields = new ArrayList<>(
                JavaConverters.asJavaCollection(inputSchema.toList()));
        boolean mapByNames = !areNamesAutoGenerated(inputFields);
        if (mapByNames) {
            for (StructField sfi : inputFields) {
                Option<Object> index = actualSchema.getFieldIndex(sfi.name());
                if (!index.isDefined() || index.isEmpty()) {
                    throw new IllegalArgumentException("Ingestion input cannot be mapped by names: "
                            + "unknown field [" + sfi.name() + "] specified on input.");
                }
                StructField sfa = actualSchema.fields()[(int) index.get()];

                if (!isAssignableFrom(sfa.dataType(), sfi.dataType())) {
                    throw new IllegalArgumentException("Ingestion input cannot be converted: "
                            + "field [" + sfi.name() + "] cannot be converted to type "
                            + sfa.dataType() + " from type " + sfi.dataType());
                }
            }
        } else {
            if (actualSchema.size() != inputSchema.size()) {
                throw new IllegalArgumentException("Ingestion input cannot be mapped by position: "
                        + "expected " + String.valueOf(actualSchema.size()) + " fields, "
                        + "got " + inputSchema.size() + " fields.");
            }
        }
        return mapByNames;
    }

    private boolean areNamesAutoGenerated(List<StructField> inputFields) {
        final Pattern p = Pattern.compile("^col[1-9][0-9]*$");
        for (StructField sf : inputFields) {
            if (!p.matcher(sf.name()).matches()) {
                return false;
            }
        }
        return true;
    }

    private boolean isAssignableFrom(DataType dst, DataType src) {
        // TODO: validate data type compatibility
        return true;
    }

}

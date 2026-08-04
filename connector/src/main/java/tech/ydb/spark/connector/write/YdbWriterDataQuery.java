package tech.ydb.spark.connector.write;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.proto.ValueProtos;
import tech.ydb.spark.connector.YdbTypes;
import tech.ydb.table.Session;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

class YdbWriterDataQuery extends YdbWriterProtobuf {
    private final String command;
    private final String tablePath;
    private final String query;
    private final ExecuteDataQuerySettings settings = new ExecuteDataQuerySettings();

    YdbWriterDataQuery(String command, String tablePath, YdbTypes types, int maxRowsCount, int maxBytesSize,
            List<ColumnEntry> columns) {
        super(types, columns, maxRowsCount, maxBytesSize);

        this.command = command;
        this.tablePath = tablePath;

        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE $input AS List<Struct<");
        sb.append(columns.stream()
                .map(c -> "`" + c.getName() + "`:" + c.getType())
                .collect(Collectors.joining(",")));
        sb.append(">>;\n");
        sb.append(command)
                .append(" INTO `")
                .append(tablePath)
                .append("`  SELECT * FROM AS_TABLE($input);");

        this.query = sb.toString();
    }

    @Override
    public String toString() {
        return "YdbWriterDataQuery[" + command + " to " + tablePath + "]";
    }

    @Override
    protected CompletableFuture<Status> writeData(Session session, ValueProtos.TypedValue data) {
        return session.executeDataQuery(query, TxControl.serializableRw(), new OneValueParams(data), settings)
                .thenApply(Result::getStatus);
    }

    private static class OneValueParams implements Params {
        private static final long serialVersionUID = 8114418145717751004L;

        private final ValueProtos.TypedValue value;

        OneValueParams(ValueProtos.TypedValue value) {
            this.value = value;
        }

        @Override
        public Map<String, ValueProtos.TypedValue> toPb() {
            return Collections.singletonMap("$input", value);
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public <T extends Type> Params put(String name, Value<T> value) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Map<String, Value<?>> values() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}

package tech.ydb.spark.connector;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * YDB type mapping settings.
 *
 * @author zinal
 */
public class YdbTypeSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean dateAsString;

    public YdbTypeSettings() {
        this.dateAsString = false;
    }

    public YdbTypeSettings(Map<String, String> options) {
        if (options == null) {
            options = Collections.emptyMap();
        }
        this.dateAsString = Boolean.parseBoolean(
                options.getOrDefault(YdbOptions.DATE_AS_STRING, "false"));
    }

    public boolean isDateAsString() {
        return dateAsString;
    }

    public void setDateAsString(boolean dateAsString) {
        this.dateAsString = dateAsString;
    }

}

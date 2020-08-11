package biz.schr.cdcdemo.util;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

public class JSONUtils {

    public static Long parseJSON(SourceRecord sourceRecord) {
        String valueString = Values.convertToString(sourceRecord.valueSchema(), sourceRecord.value());

        // parse Branka JSON
        // example: {"before":null,"after":{"brankaid":264079,"cas":2610,"soupiskaid_strelec":55104,"soupiskaid_asistent_1":55103,"soupiskaid_asistent_2":-1,"zapasid":19874,"ucastniksouteznihorocnikuid":3802,"typ":"5_5"},"source":{"version":"1.0.0.Final","connector":"mysql","name":"dbserver1","ts_ms":0,"snapshot":"true","db":"ahlcz5","table":"branka","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":4473,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1579955094872}
        if (valueString == null) {
            return null;
        }

        JsonValue value = Json.parse(valueString);
        JsonObject object = value.asObject();

        String operation = object.get("op").asString();

        if (("c".equals(operation) || "u".equals(operation))) {
            JsonObject after = object.get("after").asObject();
            return after.get("soupiskaid_strelec").asLong();
        } else {
            return null;
        }

    }
}

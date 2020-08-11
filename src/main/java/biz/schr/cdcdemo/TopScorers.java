package biz.schr.cdcdemo;

import biz.schr.cdcdemo.dto.Player;
import biz.schr.cdcdemo.util.Constants;
import biz.schr.cdcdemo.util.JSONUtils;
import biz.schr.cdcdemo.util.TopNUnique;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import io.debezium.config.Configuration;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.function.Functions.wholeItem;

/**
 * Simple Jet pipeline which consumes "goal" events from MySQL
 * using CDC and maintains top scorers
 */
public class TopScorers {

    public static final int RANKING_TABLE_SIZE = 5;

    static JobConfig getJobConfig() {
        JobConfig jc = new JobConfig();
        jc.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jc.setName("Top scorers using CDC");
        return jc;
    }

    static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("cdc-demo-connector")
                                                           .setDatabaseAddress("192.168.56.101")
                                                           .setDatabasePort(3306)
                                                           .setDatabaseUser("dbz")
                                                           .setDatabasePassword("dbz")
                                                           .setClusterName("dbserver1")
                                                           .setDatabaseWhitelist("ahlcz5")
                                                           .setTableWhitelist("ahlcz5.branka")
                                                           .setCustomProperty("include.schema.changes", "false")
                                                           .build();

        // DebeziumSources.cdc(getCDCConfiguration())

        // Continuously get and parse goal records
        p.readFrom(source)
        .withoutTimestamps()

        .map(changeEvent -> {
            if (Operation.INSERT == changeEvent.operation()
                    || Operation.SYNC == changeEvent.operation()) {
                return Long.valueOf( (int) changeEvent.value().toMap().get("soupiskaid_strelec"));
            } else {
                return null;
            }
        })

         // JSON values represent goals, parse it and extract rosterId of the goal scorer
         //.map(JSONUtils::parseJSON)

         // Map RosterId to PlayerId
         // Use the cluster cache to do the lookup. Cache is read-through to MySQL.
         .groupingKey(wholeItem())
         .mapUsingIMap(Constants.ROSTER_CACHE, (Long rosterId, Long playerId) -> playerId)

         // Update the stats for the Player
         .groupingKey(wholeItem())
         .rollingAggregate(AggregateOperations.counting())

         // Update top scorer ranking
         .rollingAggregate(TopNUnique.topNUnique(RANKING_TABLE_SIZE, ComparatorEx.comparingLong(Map.Entry::getValue)))
         .apply(TopScorers::sendUpdatesOnlyForChanges)
         .apply(TopScorers::lookupPlayers)

         .writeTo(Sinks.observable(Constants.TOP_SCORERS_OBSERVABLE));
        return p;
    }

    private static StreamStage<List<Map.Entry<Long, Long>>> sendUpdatesOnlyForChanges(StreamStage<List<Map.Entry<Long, Long>>> input) {
        return input.filterStateful(
                () -> new Object[1],
                (lastItem, item) -> {
                    if (lastItem[0] == null) {
                        lastItem[0] = item;
                        return true;
                    } else if (lastItem[0].equals(item)) {
                        return false;
                    } else {
                        lastItem[0] = item;
                        return true;
                    }
                }
        );
    }


    private static StreamStage<List<Player>> lookupPlayers(StreamStage<List<Map.Entry<Long, Long>>> input) {
        return input.mapUsingService(ServiceFactories.<Long, Player>iMapService(Constants.PLAYER_CACHE),
                (cache, item) -> {
                    return item.stream().map( e ->  {
                            Player p = cache.get( e.getKey());
                            p.setGoals(e.getValue());
                            return p;
                        })
                        .collect(Collectors.toList());
                }
        );
    }
}

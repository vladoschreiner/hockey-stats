package biz.schr.cdcdemo;

import biz.schr.cdcdemo.dto.Player;
import biz.schr.cdcdemo.util.Constants;
import biz.schr.cdcdemo.util.TopNUnique;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.retry.RetryStrategies;

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
                   .setDatabaseAddress(Config.dbIP)
                   .setDatabasePort(Config.dbPort)
                   .setDatabaseUser(Config.dbUsername)
                   .setDatabasePassword(Config.dbPassword)
                   .setClusterName("dbserver1")
                   .setDatabaseWhitelist("ahlcz5")
                   .setTableWhitelist("ahlcz5.branka")
                   .setCustomProperty("include.schema.changes", "false")
                   .setReconnectBehavior(RetryStrategies.indefinitely(5_000))
                   .build();

        // Continuously get and parse goal records
        StreamStage<Map.Entry<Long, Long>> updatedGoals = p.readFrom(source)
                   .withoutTimestamps()

                   .map(changeEvent -> {
                       if (Operation.INSERT == changeEvent.operation()
                               || Operation.SYNC == changeEvent.operation()) {
                           return (long) (int) changeEvent.value().toMap().get("soupiskaid_strelec");
                       } else {
                           return null;
                       }
                   })

                   // Map RosterId to PlayerId
                   // Use the cluster cache to do the lookup. Cache is read-through to MySQL.
                   .groupingKey(wholeItem())
                   .mapUsingIMap(Constants.ROSTER_CACHE, (Long rosterId, Long playerId) -> playerId)

                   // Update the stats for the Player
                   .groupingKey(wholeItem())
                   .rollingAggregate(AggregateOperations.counting());

        // Update Player Cache
        updatedGoals.writeTo(updatePlayerCache());

        // Update top scorer ranking and push changes to subscribers
        updatedGoals
         .rollingAggregate(TopNUnique.topNUnique(RANKING_TABLE_SIZE, ComparatorEx.comparingLong(Map.Entry::getValue)))
         .apply(TopScorers::sendUpdatesOnlyForChanges)
         .apply(TopScorers::lookupPlayers)
         .writeTo(Sinks.observable(Constants.TOP_SCORERS_OBSERVABLE));
        return p;
    }

    private static Sink<Map.Entry<Long, Long>> updatePlayerCache() {
        return Sinks.mapWithUpdating(
                Constants.PLAYER_CACHE,
                e -> e.getKey(),
                (Player player, Map.Entry<Long, Long> updatedStats) -> {
                    if (player != null && updatedStats != null) {
                        player.setGoals(updatedStats.getValue());
                        return player;
                    }

                    return null;
                }
        );
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
                (cache, item) -> item.stream().map(e ->  {
                        Player p = cache.get( e.getKey());
                        p.setGoals(e.getValue());
                        return p;
                    }).collect(Collectors.toList())
        );
    }
}

package biz.schr.cdcdemo.util;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

public class TopNUnique {

    /**
     * Returns an aggregate operation that finds the top {@code n} {@link Entry entries}
     * according to the given {@link ComparatorEx comparator}.
     * A key can be presented at most once among.
     * <p></p>
     * It outputs a sorted list with the top item in the first position.
     * <p>
     * For each key, the values must be increasing.
     * <p></p>
     * <em>Implementation note:</em> this aggregate operation does not
     * implement the {@link AggregateOperation1#deductFn() deduct} primitive.
     *
     * @param n number of top items to find
     * @param comparator compares the items
     * @param <T extends Entry> type of the input item
     */
    public static <T extends Entry> AggregateOperation1<T, PriorityQueue<T>, List<T>> topNUnique(
            int n, ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");

        ComparatorEx<? super T> comparatorReversed = comparator.reversed();
        BiConsumerEx<PriorityQueue<T>, T> accumulateFn = (queue, item) -> {

            // if the queue contains the key already remove it
            // new key is larger or equal
            Iterator<T> iter = queue.iterator();
            while (iter.hasNext()) {
                if (item.getKey().equals(iter.next().getKey())) {
                    iter.remove();
                    break;
                }
            }

            if (queue.size() == n) {
                if (comparator.compare(item, queue.peek()) <= 0) {
                    // the new item is smaller or equal to the smallest in queue
                    return;
                }

                queue.poll();
            }
            queue.offer(item);
        };
        return AggregateOperation
                .withCreate(() -> new PriorityQueue<T>(n, comparator))
                .andAccumulate(accumulateFn)
                .andCombine((left, right) -> {
                    for (T item : right) {
                        accumulateFn.accept(left, item);
                    }
                })
                .andExportFinish(queue -> {
                    ArrayList<T> res = new ArrayList<>(queue);
                    res.sort(comparatorReversed);
                    return res;
                });
    }
}

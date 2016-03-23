package com.easemob.kefu;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryEvictedListener;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * 目的:  判断是否能够使用Hazelcast的Map中的过期机制来实现一个分布式的timer
 *
 * 主要是为了判断时效性, 因为Hazelcast的Map支持在put的时候, 指定这个key的过期时间, 如果过期了,
 * 会发出 entry evicted的事件
 *
 * 具体的思路是
 *
 *  创建大量的随机的key, value是预计的过期时间的epoch秒数 (当前时间的秒数加上过期时间的秒数)
 *  然后在监听entry evicted的事件的listener中, 比较接收到这个事件的当前时间和value(预计的过期时间)的差值
 *
 * 为了实验的严谨性, 使用了不同的过期时间(ttl)
 *
 * 结果, 可以看到, 99.9%的延时都在452秒以上, 所以这种方案不可行
 *
 *
 count = 100000
 min = 3
 max = 452
 mean = 409.90
 stddev = 36.01
 median = 421.00
 75% <= 434.00
 95% <= 445.00
 98% <= 447.00
 99% <= 449.00
 99.9% <= 452.00
 *
 *
 *
 * @author stliu @ apache.org
 */
public class HazelcastMapTTLTest {
    public static void main(String[] args) throws Exception {
        final Histogram histogram = getHistogram();

        final IMap<String, Long> map = getTimerMap(histogram);
        //使用不同的ttl
        for (int ttl = 1; ttl < 11; ttl++) {

            for (int j = 0; j < 10000; j++) {
                String key = "a" + ttl + ":" + j;
                long now = Instant.now().getEpochSecond();
                long expectedExpireTime = now + ttl;
                //使用异步put来避免误差
                map.putAsync(key, expectedExpireTime, ttl, TimeUnit.SECONDS);

            }
        }

    }

    private static IMap<String, Long> getTimerMap(Histogram histogram) {
        final HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        final IMap<String, Long> map = hazelcastInstance.getMap("timer");

        map.addLocalEntryListener(new MetricsEntryEvictedListener(histogram));
        return map;
    }

    /**
     * 这里使用了dropwizard metrics库来统计
     */
    private static Histogram getHistogram() {
        final MetricRegistry metricRegistry = new MetricRegistry();
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.SECONDS)
                .build();
        reporter.start(5, TimeUnit.SECONDS);
        return metricRegistry.histogram("timer");
    }

    private static class MetricsEntryEvictedListener implements EntryEvictedListener<String, Long> {
        private final Histogram histogram;

        public MetricsEntryEvictedListener(Histogram histogram) {
            this.histogram = histogram;
        }

        public void entryEvicted(EntryEvent<String, Long> event) {
            long now = Instant.now().getEpochSecond();
            Long expectedExpireTimeInSeconds = event.getOldValue();
            histogram.update(now - expectedExpireTimeInSeconds);
        }
    }
}

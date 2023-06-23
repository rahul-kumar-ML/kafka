package org.apache.kafka.common.telemetry;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;

public class PredicateUtils {

  public static final Predicate<MetricKeyable> ALWAYS_TRUE = metricKey -> true;
  public static final Predicate<MetricKeyable> ALWAYS_FALSE = metricKey -> false;

  // If the number of unique metric names for a service is larger than this
  // constant, we'll see a performance hit as it misses the cache.
  private static final long MATCHER_CACHE_MAX_SIZE = 1000L;

  public static Predicate<MetricKeyable> buildCachingMetricsPredicate(String regexString) {
    // Configure the PatternPredicate.
    regexString = regexString.trim();

    if (regexString.isEmpty()) {
      return ALWAYS_TRUE;
    }
    Pattern pattern = Pattern.compile(regexString);

    // Putting our pattern matcher behind a cache allows for better performance
    // since pattern matching can be an expensive operation. This allows us to
    // match the metric name against the include pattern once per metric and rely
    // on this cache from that point on.
    LoadingCache<String, Boolean> cache =
        CacheBuilder.newBuilder()
            .maximumSize(MATCHER_CACHE_MAX_SIZE)
            .concurrencyLevel(1)
            .build(new CacheLoader<String, Boolean>() {
              @Override
              public Boolean load(String metricName) {
                return pattern.matcher(metricName).matches();
              }
            });
    return keyed -> cache.getUnchecked(keyed.key().name());
  }

}

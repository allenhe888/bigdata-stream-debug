package flink.debug;

import org.apache.flink.metrics.prometheus.PrometheusReporter;
import org.apache.flink.metrics.slf4j.Slf4jReporter;
import org.junit.Test;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/9/17 15:03
 */
public class TestMetrics {

    @Test
    public void test() {
        Slf4jReporter reporter = new Slf4jReporter();

        PrometheusReporter prometheusReporter = new PrometheusReporter();

    }


}

package org.apache.kafka.common.test.junit;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.PostDiscoveryFilter;

import java.util.Optional;

public class QuarantinedTestFilter implements PostDiscoveryFilter {
    private static final TestTag FLAKY_TAG = TestTag.create("flaky");

    private final QuarantinedTestSelector selector;

    public QuarantinedTestFilter() {
        selector = QuarantinedDataLoader.loadTestCatalog();
    }

    @Override
    public FilterResult apply(TestDescriptor testDescriptor) {
        Optional<TestSource> sourceOpt = testDescriptor.getSource();
        if (sourceOpt.isEmpty()) {
            return FilterResult.included("No test source");
        }

        TestSource source = sourceOpt.get();
        if (!(source instanceof MethodSource)){
            return FilterResult.included("No method");
        }

        boolean runQuarantined = System.getProperty("kafka.test.run.quarantined", "false")
            .equalsIgnoreCase("true");

        MethodSource methodSource = (MethodSource) source;

        // Check if test is quarantined
        Optional<String> reason = selector.selectQuarantined(
            methodSource.getClassName(), methodSource.getMethodName());

        // If the  only run if we've been given "kafka.test.run.quarantined=true"
        if (reason.isPresent()) {
            if (runQuarantined) {
                return FilterResult.included("run quarantined test");
            } else {
                return FilterResult.excluded(reason.get());
            }
        }

        // Only include "flaky" tag if given "kafka.test.run.quarantined=true", otherwise skip it
        boolean isFlaky = testDescriptor.getTags().contains(FLAKY_TAG);
        if (runQuarantined) {
            if (isFlaky) {
                return FilterResult.included("run flaky test");
            } else {
                return FilterResult.excluded("skip non-flaky test");
            }
        } else {
            if (isFlaky) {
                return FilterResult.excluded("skip flaky test");
            } else {
                // Base case
                return FilterResult.included(null);
            }
        }
    }
}

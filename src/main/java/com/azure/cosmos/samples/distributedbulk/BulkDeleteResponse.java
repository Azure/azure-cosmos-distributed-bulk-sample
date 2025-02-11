// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.distributedbulk;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class BulkDeleteResponse {
    private final int numberOfDocumentsDeleted;
    private final double totalRequestUnitsConsumed;
    private final Duration totalTimeTaken;
    private final List<Exception> failures;

    public BulkDeleteResponse(
        int numberOfDocumentsDeleted,
        double totalRequestUnitsConsumed,
        Duration totalTimeTaken,
        List<Exception> failures) {

        this.numberOfDocumentsDeleted = numberOfDocumentsDeleted;
        this.totalRequestUnitsConsumed = totalRequestUnitsConsumed;
        this.totalTimeTaken = totalTimeTaken;
        this.failures = failures;
    }

    public int getNumberOfDocumentsDeleted() {
        return this.numberOfDocumentsDeleted;
    }

    public double getTotalRequestUnitsConsumed() {
        return this.totalRequestUnitsConsumed;
    }

    public Duration getTotalTimeTaken() {
        return this.totalTimeTaken;
    }

    public List<Exception> getErrors() {
        return Collections.unmodifiableList(this.failures);
    }
}
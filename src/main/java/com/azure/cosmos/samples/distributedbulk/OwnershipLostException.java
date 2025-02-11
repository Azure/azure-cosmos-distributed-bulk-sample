// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.distributedbulk;

public class OwnershipLostException extends RuntimeException {
    public OwnershipLostException(String jobId, String blobName, int batchIndex) {
        super("Machine '"
            + Main.getMachineId()
            + "'Lost ownership of batch '" + jobId + "|" + blobName + "|" + batchIndex + "'.");
    }
}

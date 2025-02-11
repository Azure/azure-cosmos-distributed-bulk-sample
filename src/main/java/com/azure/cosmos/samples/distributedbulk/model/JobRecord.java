// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JobRecord {
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final String id;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final String pk;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final List<String> inputFiles;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonProperty("recordType")
    private final String recordType = "J";

    @JsonCreator
    public JobRecord(
        @JsonProperty("id") String id,
        @JsonProperty("pk") String partitionKeyValue,
        @JsonProperty("inputFiles") List<String> inputFiles) {

        Objects.requireNonNull(id, "Argument 'id' must not be null.");
        Objects.requireNonNull(partitionKeyValue, "Argument 'partitionKeyValue' must not be null.");
        this.id = id;
        this.pk = partitionKeyValue;
        this.inputFiles = inputFiles != null ? inputFiles : new ArrayList<>();
    }

    public String getId() {
        return this.id;
    }

    @JsonProperty("pk")
    public String getPartitionKeyValue() {
        return this.pk;
    }

    public List<String> getInputFiles() {
        return this.inputFiles;
    }

    public String getRecordType() { return this.recordType; }
}

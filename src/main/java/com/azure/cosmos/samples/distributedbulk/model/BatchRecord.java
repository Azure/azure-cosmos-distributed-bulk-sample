// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.Instant;
import java.util.Objects;

public class BatchRecord {
    @JsonSerialize(using = LongToStringSerializer.class)
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final long offset;

    @JsonSerialize(using = LongToStringSerializer.class)
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final long recordCount;

    @JsonSerialize(using = IntegerToStringSerializer.class)
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final int index;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String owningWorker;

    @JsonDeserialize(using = StringToTimeStampDeserializer.class)
    @JsonSerialize(using = TimeStampToStringSerializer.class)
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Instant owningWorkerLastModified;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final String id;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final String pk;

    @JsonProperty("_etag")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String etag;

    @JsonProperty("recordType")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final String recordType = "B";

    @JsonProperty("status")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private IngestionStatus status;

    @JsonDeserialize(using = StringToDoubleDeserializer.class)
    @JsonSerialize(using = DoubleToStringSerializer.class)
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private double estimatedProgress;

    @JsonCreator
    public BatchRecord(
        @JsonProperty("id") String id,
        @JsonProperty("pk") String partitionKeyValue,
        @JsonProperty("index") @JsonDeserialize(using = StringToIntegerDeserializer.class) Integer index,
        @JsonProperty("recordCount") @JsonDeserialize(using = StringToLongDeserializer.class) Long recordCount,
        @JsonProperty("offset") @JsonDeserialize(using = StringToLongDeserializer.class) Long offset) {

        Objects.requireNonNull(id, "Argument 'id' must not be null.");
        Objects.requireNonNull(partitionKeyValue, "Argument 'partitionKeyValue' must not be null.");
        this.id = id;
        this.pk = partitionKeyValue;
        this.recordCount = recordCount != null ? recordCount : 0L;
        this.index = index != null ? index : 0;
        this.offset = offset != null ? offset : 0L;
        this.owningWorker = "";
        this.owningWorkerLastModified = Instant.EPOCH;
    }

    public long getOffset() {
        return this.offset;
    }

    public long getRecordCount() {
        return this.recordCount;
    }

    public int getIndex() { return this.index; }

    public String getOwningWorker() {
        return this.owningWorker;
    }

    public void setOwningWorker(String value) {
        this.owningWorker = value;
    }

    public Instant getOwningWorkerLastModified() {
        return this.owningWorkerLastModified;
    }

    public void setOwningWorkerLastModified(Instant value) {
        this.owningWorkerLastModified = value;
    }

    public String getId() {
        return this.id;
    }

    @JsonProperty("pk")
    public String getPartitionKeyValue() {
        return this.pk;
    }

    public String getEtag() { return this.etag; }

    public String getRecordType() { return this.recordType; }

    public void setEtag(String newEtag) {
        this.etag = newEtag;
    }

    public double getEstimatedProgress() {
        return this.estimatedProgress;
    }

    public void setEstimatedProgress(double value) {
        this.estimatedProgress = value;
    }

    public IngestionStatus getStatus() {
        return this.status;
    }

    public void setStatus(IngestionStatus value) {
        this.status = value != null ? value : IngestionStatus.NONE;
    }
}

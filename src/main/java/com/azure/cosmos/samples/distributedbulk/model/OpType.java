// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.samples.distributedbulk.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum OpType {
    UPSERT("upsert"),
    CREATE("create"),
    DELETE("delete");

    private final String value;

    OpType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static OpType fromValue(String value) {
        for (OpType status : OpType.values()) {
            if (status.value.equalsIgnoreCase(value)) {
                return status;
            }
        }

        throw new IllegalArgumentException("Unknown enum value: " + value);
    }
}

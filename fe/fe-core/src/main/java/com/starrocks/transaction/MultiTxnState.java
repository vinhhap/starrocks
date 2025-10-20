// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

public class MultiTxnState implements Writable {

    public static class MultiTxnStateComparator implements Comparator<MultiTxnState> {
        @Override
        public int compare(MultiTxnState t1, MultiTxnState t2) {
            return Long.compare(t2.getTxnId(), t1.getTxnId());
        }
    }

    public static final MultiTxnState.MultiTxnStateComparator MULTI_TXN_COMPARATOR = new MultiTxnState.MultiTxnStateComparator();

    @SerializedName("connId")
    private final int connId;
    @SerializedName("txnId")
    private final long txnId;
    @SerializedName("writtenTables")
    private final Set<TableName> writtenTables;
    @SerializedName("status")
    private TransactionStatus status;
    @SerializedName("prepareTime")
    private final long prepareTime;
    @SerializedName("finishTime")
    private long finishTime = -1;

    public MultiTxnState(int connId, long txnId) {
        this.connId = connId;
        this.txnId = txnId;
        this.writtenTables = new HashSet<>();
        this.status = TransactionStatus.PREPARE;
        this.prepareTime = System.currentTimeMillis();
    }

    boolean isExpired(long expiredTime) {
        return this.finishTime > 0 && this.finishTime < expiredTime;
    }

    boolean hasBlockingTable(TableName tblName) {
        return this.writtenTables.contains(tblName);
    }

    public void addWrittenTable(TableName tblName) {
        this.writtenTables.add(tblName);
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public int getConnId() {
        return connId;
    }

    public long getTxnId() {
        return txnId;
    }

    public Set<TableName> getWrittenTables() {
        return writtenTables;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public long getPrepareTime() {
        return prepareTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MultiTxnState read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), MultiTxnState.class);
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}

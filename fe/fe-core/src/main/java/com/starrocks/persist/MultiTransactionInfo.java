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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/GlobalTransactionMgr.java

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

package com.starrocks.persist;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.transaction.GlobalTransactionMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MultiTransactionInfo implements Writable {
    @SerializedName("txnId")
    private long txnId;
    @SerializedName("commitTxnInfo")
    private GlobalTransactionMgr.CommitTxnInfo commitTxnInfo;

    public MultiTransactionInfo() {
        // for persist
    }

    public MultiTransactionInfo(long txnId, GlobalTransactionMgr.CommitTxnInfo txnInfo) {
        this.txnId = txnId;
        this.commitTxnInfo = txnInfo;
    }

    public long getTxnId() {
        return txnId;
    }

    public GlobalTransactionMgr.CommitTxnInfo getCommitTxnInfo() {
        return commitTxnInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static MultiTransactionInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), MultiTransactionInfo.class);
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}

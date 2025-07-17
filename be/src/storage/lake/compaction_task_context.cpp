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

#include "storage/lake/compaction_task_context.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "runtime/exec_env.h"
#include "storage/lake/rowset.h"
#include "storage/olap_common.h"
#include "util/brpc_stub_cache.h"

namespace starrocks::lake {

static constexpr long TIME_UNIT_NS_PER_SECOND = 1000000000;
static constexpr long BYTES_UNIT_MB = 1048576;
static constexpr long DEFAULT_COMPACTION_TIMEOUT_MS = 60000;

void CompactionTaskStats::collect(const OlapReaderStatistics& reader_stats) {
    io_ns_remote = reader_stats.io_ns_remote;
    io_ns_local_disk = reader_stats.io_ns_read_local_disk;
    io_bytes_read_remote = reader_stats.compressed_bytes_read_remote;
    io_bytes_read_local_disk = reader_stats.compressed_bytes_read_local_disk;
    segment_init_ns = reader_stats.segment_init_ns;
    column_iterator_init_ns = reader_stats.column_iterator_init_ns;
    io_count_local_disk = reader_stats.io_count_local_disk;
    io_count_remote = reader_stats.io_count_remote;
    // read segment count is managed else where
    // read_segment_count = reader_stats.segments_read_count;
}

void CompactionTaskStats::collect(const OlapWriterStatistics& writer_stats) {
    write_segment_count = writer_stats.segment_count;
    write_segment_bytes = writer_stats.bytes_write;
}

CompactionTaskStats CompactionTaskStats::operator+(const CompactionTaskStats& that) const {
    CompactionTaskStats diff = *this;
    diff.io_ns_remote += that.io_ns_remote;
    diff.io_ns_local_disk += that.io_ns_local_disk;
    diff.io_bytes_read_remote += that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk += that.io_bytes_read_local_disk;
    diff.segment_init_ns += that.segment_init_ns;
    diff.column_iterator_init_ns += that.column_iterator_init_ns;
    diff.io_count_local_disk += that.io_count_local_disk;
    diff.io_count_remote += that.io_count_remote;
    // read segment count is managed else where
    // diff.read_segment_count += that.read_segment_count;
    diff.write_segment_count += that.write_segment_count;
    diff.write_segment_bytes += that.write_segment_bytes;
    diff.in_queue_time_sec += that.in_queue_time_sec;
    diff.pk_sst_merge_ns += that.pk_sst_merge_ns;
    diff.input_file_size += that.input_file_size;
    return diff;
}

CompactionTaskStats CompactionTaskStats::operator-(const CompactionTaskStats& that) const {
    CompactionTaskStats diff = *this;
    diff.io_ns_remote -= that.io_ns_remote;
    diff.io_ns_local_disk -= that.io_ns_local_disk;
    diff.io_bytes_read_remote -= that.io_bytes_read_remote;
    diff.io_bytes_read_local_disk -= that.io_bytes_read_local_disk;
    diff.segment_init_ns -= that.segment_init_ns;
    diff.column_iterator_init_ns -= that.column_iterator_init_ns;
    diff.io_count_local_disk -= that.io_count_local_disk;
    diff.io_count_remote -= that.io_count_remote;
    // read segment count is managed else where
    // diff.read_segment_count -= that.read_segment_count;
    diff.write_segment_count -= that.write_segment_count;
    diff.write_segment_bytes -= that.write_segment_bytes;
    diff.in_queue_time_sec -= that.in_queue_time_sec;
    diff.pk_sst_merge_ns -= that.pk_sst_merge_ns;
    diff.input_file_size -= that.input_file_size;
    return diff;
}

std::string CompactionTaskStats::to_json_stats() {
    rapidjson::Document root;
    root.SetObject();
    auto& allocator = root.GetAllocator();
    // add stats
    root.AddMember("read_local_sec", rapidjson::Value(io_ns_local_disk / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_local_mb", rapidjson::Value(io_bytes_read_local_disk / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_sec", rapidjson::Value(io_ns_remote / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("read_remote_mb", rapidjson::Value(io_bytes_read_remote / BYTES_UNIT_MB), allocator);
    root.AddMember("read_remote_count", rapidjson::Value(io_count_remote), allocator);
    root.AddMember("read_local_count", rapidjson::Value(io_count_local_disk), allocator);
    root.AddMember("segment_init_sec", rapidjson::Value(segment_init_ns / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("column_iterator_init_sec", rapidjson::Value(column_iterator_init_ns / TIME_UNIT_NS_PER_SECOND),
                   allocator);
    root.AddMember("read_segment_count", rapidjson::Value(read_segment_count), allocator);
    root.AddMember("write_segment_count", rapidjson::Value(write_segment_count), allocator);
    root.AddMember("write_segment_mb", rapidjson::Value(write_segment_bytes / BYTES_UNIT_MB), allocator);
    root.AddMember("in_queue_sec", rapidjson::Value(in_queue_time_sec), allocator);
    root.AddMember("pk_sst_merge_sec", rapidjson::Value(pk_sst_merge_ns / TIME_UNIT_NS_PER_SECOND), allocator);
    root.AddMember("input_file_size", rapidjson::Value(input_file_size), allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    return {strbuf.GetString()};
}

void CompactionTaskContext::split_rowset_ranges_and_trigger_compact_rpc(std::vector<RowsetPtr>* input_rowsets,
                                                                        const CompactionAlgorithm& input_algorithm) {
    LOG(INFO) << "split rowset ranges and trigger compact rpc, tablet_id=" << tablet_id << ", txn_id=" << txn_id
              << ", version=" << version << ", total_rowsets=" << input_rowsets->size()
              << ", input_algorithm=" << input_algorithm;
    DCHECK(enable_rs_range_compaction && !input_rowsets->empty());
    int executor_node_num = executor_node_infos.size();
    const int total_rowsets = input_rowsets->size();
    const int split_num = std::max(1, total_rowsets / (executor_node_num + 1));
    DCHECK(split_num >= 1);

    // all beginning rowsets will be split into a group (coordinator)
    std::vector<RowsetPtr> coordinator_rowsets(input_rowsets->begin(), input_rowsets->begin() + split_num);

    int start_index = split_num;
    for (int i = 0; i < executor_node_num && start_index < total_rowsets; ++i) {
        // build remote rs range compact request
        CompactRequest request;
        request.add_tablet_ids(tablet_id);
        request.set_txn_id(txn_id);
        request.set_version(version);
        request.set_algorithm(static_cast<int>(input_algorithm));

        std::stringstream ss;
        ss << "[";

        int end_index;
        if (i == executor_node_num - 1) {
            // all remaining rowset will be split into a group
            end_index = total_rowsets;
        } else {
            end_index = std::min(start_index + split_num, total_rowsets);
        }

        for (int j = start_index; j < end_index; j++) {
            auto rs_id = (*input_rowsets)[j]->id();
            request.add_input_rowset_ids(rs_id);
            // for log
            if (j != end_index - 1) {
                ss << rs_id << ",";
            } else {
                ss << rs_id << "]";
            }
        }

        closure_vec.emplace_back(new ReusableClosure<CompactResponse>());
        auto closure = closure_vec.back();
        closure->ref();
        closure->reset();
        closure->cntl.set_timeout_ms(config::lake_rowset_range_compaction_rpc_timeout_sec * 1000);
        SET_IGNORE_OVERCROWDED(closure->cntl, compaction);

        auto node_info = executor_node_infos[i];
#ifndef BE_TEST
        auto stub = ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(node_info.host(), node_info.port());
        if (stub == nullptr) {
            LOG(WARNING) << "Failed to Connect node: " << node_info.host() << ":" << node_info.port();
            continue;
        }
        stub->lake_compact_rowset_range(&closure->cntl, &request, &closure->result, closure);
#endif

        LOG(INFO) << "Send rowset range scan compact request to " << node_info.host() << ":" << node_info.port()
                  << ", rowset ids: " << ss.str() << ", tablet_id=" << tablet_id << ", txn_id=" << txn_id;

        start_index = end_index;
    }
    all_input_rowsets = *input_rowsets; // make a copy into context
    *input_rowsets = std::move(coordinator_rowsets);
}

// todo: support partial success
Status CompactionTaskContext::wait_rs_range_compact_response() {
    DCHECK(enable_rs_range_compaction);
    if (closure_vec.empty()) {
        LOG(INFO) << "No need to wait rowset range compaction response, txn_id: " << txn_id
                  << ", tablet_id: " << tablet_id;
        return Status::OK();
    }
    for (auto& closure : closure_vec) {
        if (closure->join()) {
            if (closure->cntl.Failed()) {
                auto st = Status::InternalError(closure->cntl.ErrorText());
                LOG(WARNING) << "Failed to send rowset range compation rpc, err=" << st;
                return st;
            }
            Status st(closure->result.status());
            if (!st.ok()) {
                LOG(WARNING) << "Failed to send rowset range compation rpc, err=" << st;
                return st;
            }
        }
    }
    LOG(INFO) << "All rowset range compaction response received, txn_id: " << txn_id << ", tablet_id: " << tablet_id;
    return Status::OK();
}
} // namespace starrocks::lake
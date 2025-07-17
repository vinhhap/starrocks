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

#include "storage/lake/compaction_task.h"

#include "gen_cpp/Status_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/exec_env.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"

namespace starrocks::lake {

CompactionTask::CompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                               CompactionTaskContext* context, std::shared_ptr<const TabletSchema> tablet_schema)
        : _txn_id(context->txn_id),
          _tablet(std::move(tablet)),
          _input_rowsets(std::move(input_rowsets)),
          _mem_tracker(std::make_unique<MemTracker>(MemTracker::COMPACTION, -1,
                                                    "Compaction-" + std::to_string(_tablet.metadata()->id()),
                                                    GlobalEnv::GetInstance()->compaction_mem_tracker())),
          _context(context),
          _tablet_schema(std::move(tablet_schema)) {}

Status CompactionTask::execute_index_major_compaction(TxnLogPB* txn_log) {
    if (_tablet.get_schema()->keys_type() == KeysType::PRIMARY_KEYS) {
        SCOPED_RAW_TIMER(&_context->stats->pk_sst_merge_ns);
        auto metadata = _tablet.metadata();
        if (metadata->enable_persistent_index() &&
            metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE) {
            RETURN_IF_ERROR(_tablet.tablet_manager()->update_mgr()->execute_index_major_compaction(*metadata, txn_log));
            if (txn_log->has_op_compaction() && !txn_log->op_compaction().input_sstables().empty()) {
                size_t total_input_sstable_file_size = 0;
                for (const auto& input_sstable : txn_log->op_compaction().input_sstables()) {
                    total_input_sstable_file_size += input_sstable.filesize();
                }
                _context->stats->input_file_size += total_input_sstable_file_size;
            }
            return Status::OK();
        }
    }
    return Status::OK();
}

Status CompactionTask::fill_compaction_segment_info(TxnLogPB_OpCompaction* op_compaction, TabletWriter* writer) {
    if (_context->enable_rs_range_compaction) {
        // todo support partial success
        for (auto& rowset : _context->all_input_rowsets) {
            op_compaction->add_input_rowsets(rowset->id());
        }
        // add files first
        for (auto& file : writer->files()) {
            op_compaction->mutable_output_rowset()->add_segments(file.path);
            op_compaction->mutable_output_rowset()->add_segment_size(file.size.value());
            op_compaction->mutable_output_rowset()->add_segment_encryption_metas(file.encryption_meta);
        }
        // add writer output info
        auto num_rows = op_compaction->mutable_output_rowset()->num_rows() + writer->num_rows();
        op_compaction->mutable_output_rowset()->set_num_rows(num_rows);
        auto data_size = op_compaction->mutable_output_rowset()->data_size() + writer->data_size();
        op_compaction->mutable_output_rowset()->set_data_size(data_size);
        op_compaction->mutable_output_rowset()->set_overlapped(true);
        return Status::OK();
    }

    for (auto& rowset : _input_rowsets) {
        op_compaction->add_input_rowsets(rowset->id());
    }

    // check last rowset whether this is a partial compaction
    if (_tablet_schema->keys_type() != KeysType::PRIMARY_KEYS && _input_rowsets.size() > 0 &&
        _input_rowsets.back()->partial_segments_compaction()) {
        uint64_t uncompacted_num_rows = 0;
        uint64_t uncompacted_data_size = 0;
        RETURN_IF_ERROR(_input_rowsets.back()->add_partial_compaction_segments_info(
                op_compaction, writer, uncompacted_num_rows, uncompacted_data_size));
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows() + uncompacted_num_rows);
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size() + uncompacted_data_size);
        op_compaction->mutable_output_rowset()->set_overlapped(true);
    } else {
        for (auto& file : writer->files()) {
            op_compaction->mutable_output_rowset()->add_segments(file.path);
            op_compaction->mutable_output_rowset()->add_segment_size(file.size.value());
            op_compaction->mutable_output_rowset()->add_segment_encryption_metas(file.encryption_meta);
        }
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows());
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size());
        op_compaction->mutable_output_rowset()->set_overlapped(false);
        op_compaction->mutable_output_rowset()->set_next_compaction_offset(0);
    }
    return Status::OK();
}

Status CompactionTask::finish_rs_range_compaction(TabletWriter* writer) {
    DCHECK(_context->is_rs_range_compaction_executor_task);
    auto response = _context->callback->get_response();
    response->mutable_status()->set_status_code(TStatusCode::OK);
    response->set_num_output_bytes(writer->data_size());
    response->set_num_output_rows(writer->num_rows());

    std::stringstream ss;
    std::vector<FileInfoPB> file_info_pb_vec;
    for (auto file : writer->files()) {
        FileInfoPB* file_info_pb = response->add_files();
        file_info_pb->set_path(file.path);
        file_info_pb->set_size(file.size.value());
        file_info_pb->set_encryption_meta(file.encryption_meta);
        ss << file.path << ":" << file.size.value() << " ";
    }
    response->mutable_input_rowset_ids()->Reserve(static_cast<int>(_context->current_rs_rowset_ids.size()));
    for (auto id : _context->current_rs_rowset_ids) {
        response->add_input_rowset_ids(id);
    }
    LOG(INFO) << "finished rs range compaction, tablet_id=" << _tablet.metadata()->id() << ", txn_id=" << _txn_id
              << ", output num_rows=" << writer->num_rows() << ", output data_size=" << writer->data_size()
              << ", output segments=" << ss.str();
    return Status::OK();
}

Status CompactionTask::fill_rs_range_compaction_segment_info(TxnLogPB_OpCompaction* op_compaction,
                                                             CompactResponse* response) {
    if (response != nullptr) {
        for (auto& file : response->files()) {
            op_compaction->mutable_output_rowset()->add_segments(file.path());
            op_compaction->mutable_output_rowset()->add_segment_size(file.size());
            op_compaction->mutable_output_rowset()->add_segment_encryption_metas(file.encryption_meta());
        }

        auto num_rows = response->num_output_rows();
        if (op_compaction->mutable_output_rowset()->has_num_rows()) {
            num_rows += op_compaction->mutable_output_rowset()->num_rows();
        }
        op_compaction->mutable_output_rowset()->set_num_rows(num_rows);

        auto data_size = response->num_output_bytes();
        if (op_compaction->mutable_output_rowset()->has_data_size()) {
            data_size += op_compaction->mutable_output_rowset()->data_size();
        }
        op_compaction->mutable_output_rowset()->set_data_size(data_size);
    }

    return Status::OK();
}

} // namespace starrocks::lake

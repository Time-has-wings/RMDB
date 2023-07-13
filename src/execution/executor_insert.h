/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle *fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_;                   // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size())
        {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override
    {
        if (context_->txn_->get_txn_mode())
        {
            bool res = (context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd()));
            if (res == false)
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++)
        {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            val.value_change(col.type);
            if (col.type != val.type)
            {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        if (tab_.indexes.size() == 0)
        {
            rid_ = fh_->insert_record(rec.data, context_);
            // modify wset
            WriteRecord *wrec = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
            context_->txn_->append_write_record(wrec);
            InsertLogRecord insert_log(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
            insert_log.prev_lsn_ = context_->txn_->get_prev_lsn();
            context_->log_mgr_->add_log_to_buffer(&insert_log);
            context_->log_mgr_->flush_log_to_disk();
            context_->txn_->set_prev_lsn(insert_log.lsn_);
            return nullptr;
        }
        // Insert into index judge
        for (size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char key[index.col_tot_len];
            int offset = 0;
            for (size_t i = 0; i < index.col_num; ++i)
            {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            if (ih->get_value(key, nullptr, nullptr))
                throw IndexEnrtyExistsError();
        }
        // 写日志
        Rid rid_insert = {-1, -1};
        InsertLogRecord insert_log(context_->txn_->get_transaction_id(), rec, rid_insert, tab_name_); // rid先草草设置为{-1, -1}
        insert_log.prev_lsn_ = context_->txn_->get_prev_lsn();                                        // 设置日志的prev_lsn
        // 写记录
        rid_ = fh_->insert_record(rec.data, context_);
        insert_log.rid_ = rid_;                             // 重新设置log的rid
        context_->log_mgr_->add_log_to_buffer(&insert_log); // 写入日志缓冲区
        context_->log_mgr_->flush_log_to_disk();            // 写入日志缓冲区
        // modify wset
        WriteRecord *wrec = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
        context_->txn_->append_write_record(wrec);
        for (size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char key[index.col_tot_len];
            int offset = 0;
            for (size_t i = 0; i < index.col_num; ++i)
            {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->insert_entry(key, rid_, context_->txn_);
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};
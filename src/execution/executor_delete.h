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

class DeleteExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;                  // 表的元数据
    std::vector<Condition> conds_; // delete的条件
    RmFileHandle *fh_;             // 表的数据文件句柄
    std::vector<Rid> rids_;        // 需要删除的记录的位置
    std::string tab_name_;         // 表名称
    SmManager *sm_manager_;

public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    virtual std::string getType() { return "DeleteExecutor"; };
    std::unique_ptr<RmRecord> Next() override
    {
        if (context_->txn_->get_txn_mode())
        {
            bool res = (context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd()));
            if (res == false)
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        for (auto rid : rids_)
        {
            auto rec = fh_->get_record(rid, context_);
            DeleteLogRecord delete_log(context_->txn_->get_transaction_id(), *rec, rid, tab_name_);
            delete_log.prev_lsn_ = context_->txn_->get_prev_lsn();
            context_->log_mgr_->add_log_to_buffer(&delete_log);
            context_->txn_->set_prev_lsn(delete_log.lsn_);
            for (auto &index : tab_.indexes)
            {
                auto ihs = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
                char key[index.col_tot_len];
                int offset = 0;
                for (size_t i = 0; i < index.cols.size(); i++)
                {
                    memcpy(key + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ihs->delete_entry(key, context_->txn_);
            }
            RmRecord delete_record(rec->size);
            memcpy(delete_record.data, rec->data, rec->size);
            fh_->delete_record(rid, context_);
            WriteRecord *wrec = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, delete_record);
            context_->txn_->append_write_record(wrec);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
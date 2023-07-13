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

class UpdateExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override
    {
        for (auto &clause : set_clauses_)
        {
            auto col = tab_.get_col(clause.lhs.col_name);
            clause.rhs.value_change(col->type);
            if (clause.rhs.type != col->type)
            {
                throw IncompatibleTypeError(coltype2str(col->type), coltype2str(clause.rhs.type));
            }
            switch (clause.rhs.type)
            {
            case TYPE_INT:
            case TYPE_FLOAT:
                clause.rhs.init_raw(4);
                break;
            case TYPE_STRING:
                clause.rhs.init_raw(col->len);
                break;
            case TYPE_DATETIME:
                clause.rhs.init_raw(col->len);
                break;
            case TYPE_BIGINT:
                clause.rhs.init_raw(8);
                break;
            }
        }
        if (context_->txn_->get_txn_mode())
        {
            for (auto &rid : rids_)
            {
                bool res = (context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd()));
                if (res == false)
                    throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
        for (auto &index : tab_.indexes)
        {
            char key_old[index.col_tot_len];
            char key[index.col_tot_len];
            for (auto &rid : rids_)
            {
                std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
                char dat[rec->size];
                memcpy(dat, rec->data, rec->size);
                for (auto &clause : set_clauses_)
                {
                    auto col = tab_.get_col(clause.lhs.col_name);
                    memcpy(dat + col->offset, clause.rhs.raw->data, col->len);
                }
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i)
                {
                    memcpy(key + offset, dat + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                std::memcpy(key_old, key, index.col_tot_len);
                int cnt = 0;
                for (auto &rid : rids_)
                {
                    std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
                    char dat[rec->size];
                    memcpy(dat, rec->data, rec->size);
                    for (auto &clause : set_clauses_)
                    {
                        auto col = tab_.get_col(clause.lhs.col_name);
                        memcpy(dat + col->offset, clause.rhs.raw->data, col->len);
                    }
                    int offset = 0;
                    for (size_t i = 0; i < index.col_num; ++i)
                    {
                        memcpy(key + offset, dat + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }
                    if (memcmp(key_old, key, index.col_tot_len) == 0)
                        cnt++;
                    if (cnt >= 2)
                        throw IndexEnrtyExistsError();
                }
            }
        }
        for (auto rid : rids_)
        {
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
            RmRecord update_record(rec->size);
            memcpy(update_record.data, rec->data, rec->size);
            for (auto &clause : set_clauses_)
            {
                auto col = tab_.get_col(clause.lhs.col_name);
                memcpy(update_record.data + col->offset, clause.rhs.raw->data, col->len);
            }
            UpdateLogRecord update_log(context_->txn_->get_transaction_id(), *(rec.get()), update_record, rid, tab_name_);
            update_log.prev_lsn_ = context_->txn_->get_prev_lsn();
            context_->log_mgr_->add_log_to_buffer(&update_log);
            context_->txn_->set_prev_lsn(update_log.lsn_);
        }
        context_->log_mgr_->flush_log_to_disk();
        for (auto &rid : rids_)
        {
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
            RmRecord origin_record(rec->size);
            memcpy(origin_record.data, rec->data, rec->size);
            char dat[rec->size];
            memcpy(dat, rec->data, rec->size);
            for (auto &clause : set_clauses_)
            {
                auto col = tab_.get_col(clause.lhs.col_name);
                memcpy(dat + col->offset, clause.rhs.raw->data, col->len);
            }
            for (auto &index : tab_.indexes)
            {
                auto ihs = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
                char *key1 = new char[index.col_tot_len];
                char *key2 = new char[index.col_tot_len];
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i)
                {
                    memcpy(key1 + offset, dat + index.cols[i].offset, index.cols[i].len);
                    memcpy(key2 + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                if (memcmp(key1, key2, index.col_tot_len) == 0)
                    continue;
                else if (ihs->get_value(key1, nullptr, nullptr))
                    throw IndexEnrtyExistsError();
                else
                {
                    ihs->delete_entry(key2, context_->txn_);
                    ihs->insert_entry(key1, rid, context_->txn_);
                }
            }

            fh_->update_record(rid, dat, context_);
            WriteRecord *wrec = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, origin_record);
            context_->txn_->append_write_record(wrec);
        }

        return nullptr;
    }

    Rid &
    rid() override
    {
        return _abstract_rid;
    }
};
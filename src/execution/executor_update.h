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
    char **rids_data;
    size_t rec_size;

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
        rids_data = new char *[rids_.size()];
    }
    std::unique_ptr<RmRecord> Next() override
    {
        if (context_->txn_->get_txn_mode())
        {
            for (auto &rid : rids_)
            {
                bool res = (context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd()));
                if (res == false)
                    throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }
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
        for (int i = 0; i < rids_.size(); i++)
        {
            auto &rid = rids_.at(i);
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
            rec_size = rec->size;
            *(rids_data + i) = new char[rec->size];
            char *dat = *(rids_data + i);
            memcpy(dat, rec->data, rec->size);
            for (auto &clause : set_clauses_)
            {
                auto col = tab_.get_col(clause.lhs.col_name);
                memcpy(dat + col->offset, clause.rhs.raw->data, col->len);
            }
        }
        for (auto &index : tab_.indexes)
        {
            for (int i = 0; i < rids_.size() - 1; i++)
            {
                for (int j = i + 1; j < rids_.size(); j++)
                {
                    int offset = 0;
                    bool f = true;
                    for (size_t k = 0; k < index.col_num; ++k)
                    {
                        if (memcmp(*(rids_data + i) + index.cols[k].offset, *(rids_data + j) + index.cols[k].offset, index.cols[k].len))
                        {
                            f = false;
                            break;
                        }
                        offset += index.cols[i].len;
                    }
                    if (f)
                        throw IndexEnrtyExistsError();
                }
            }
        }
        for (int i = 0; i < rids_.size(); i++)
        {
            auto &rid = rids_.at(i);
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
            UpdateLogRecord update_log(context_->txn_->get_transaction_id(), *(rec.get()), *(rids_data + i), rid, tab_name_);
            update_log.prev_lsn_ = context_->txn_->get_prev_lsn();
            context_->log_mgr_->add_log_to_buffer(&update_log);
            context_->txn_->set_prev_lsn(update_log.lsn_);
        }
        for (int i = 0; i < rids_.size(); i++)
        {
            auto &rid = rids_.at(i);
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);

            for (auto &index : tab_.indexes)
            {
                auto ihs = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
                char update[index.col_tot_len];
                char orign[index.col_tot_len];
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i)
                {
                    memcpy(update + offset, *(rids_data + i) + index.cols[i].offset, index.cols[i].len);
                    memcpy(orign + offset, rec->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                if (memcmp(update, orign, index.col_tot_len) == 0)
                    continue;
                else if (ihs->get_value(update, nullptr, nullptr))
                    throw IndexEnrtyExistsError();
                else
                {
                    ihs->delete_entry(orign, context_->txn_);
                    ihs->insert_entry(update, rid, context_->txn_);
                }
            }
            fh_->update_record(rid, *(rids_data + i), context_);
            WriteRecord *wrec = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec.get());
            context_->txn_->append_write_record(wrec);
        }
        for (int i = 0; i < rids_.size(); i++)
            delete[] *(rids_data + i);
        return nullptr;
    }
    Rid &rid()
    {
        return _abstract_rid;
    }
};
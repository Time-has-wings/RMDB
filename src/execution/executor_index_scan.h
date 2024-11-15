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

class IndexScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_;             // 表名称
    TabMeta tab_;                      // 表的元数据
    std::vector<Condition> conds_;     // 扫描条件
    RmFileHandle *fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // 需要读取的字段
    size_t len_;                       // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_; // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_; // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                     // index scan涉及到的索引元数据
    Rid rid_t;
    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    std::shared_ptr<RmPageHandle> cur_page;
    size_t idx = 0;
    SmManager *sm_manager_;

public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                      Context *context)
    {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ},
            {OP_NE, OP_NE},
            {OP_LT, OP_GT},
            {OP_GT, OP_LT},
            {OP_LE, OP_GE},
            {OP_GE, OP_LE},
        };

        for (auto &cond : conds_)
        {
            if (cond.lhs_col.tab_name != tab_name_)
            {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }
    const std::vector<ColMeta> &cols() const
    {
        return cols_;
    };
    bool is_end() const override { return scan_->is_end(); }
    size_t tupleLen() const { return len_; };
    std::string getType() { return "SeqScanExecutor"; };
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord &target)
    {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        auto lhs_val = get_value(target, *lhs_col);
        Value rhs_val;
        if (!cond.is_rhs_val)
        {
            ColMeta rhs_col = *get_col(rec_cols, cond.rhs_col);
            rhs_val = get_value(target, rhs_col);
        }
        else
        {
            rhs_val = cond.rhs_val;
        }
        return compare_value(lhs_val, rhs_val, cond.op);
    }
    void beginTuple() override
    {
        // 给表上S锁
        if (context_->txn_->get_txn_mode() && context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd()) == false)
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        Iid lower{-1, -1}, upper{-1, -1};
        char rhs_key[index_meta_.col_tot_len];
        std::memset(rhs_key, 0, sizeof(char) * index_meta_.col_tot_len);
        int off_set = 0;
        for (int i = 0; i < fed_conds_.size(); i++)
        {
            auto cond = fed_conds_[i];
            if (cond.is_rhs_val && cond.op != OP_NE && cond.lhs_col.col_name == index_col_names_[i])
            {
                idx = i + 1;
                std::memcpy(rhs_key + off_set, cond.rhs_val.raw->data, cond.rhs_val.raw->size);
                off_set += cond.rhs_val.raw->size;
                switch (cond.op)
                {
                case OP_EQ:
                    lower = ih->lower_bound(rhs_key);
                    upper = ih->upper_bound(rhs_key);
                    continue;
                    break;
                case OP_LT:
                    upper = ih->lower_bound(rhs_key);
                    break;
                case OP_GT:
                    lower = ih->upper_bound(rhs_key);
                    break;
                case OP_LE:
                    upper = ih->upper_bound(rhs_key);
                    break;
                case OP_GE:
                    lower = ih->lower_bound(rhs_key);
                    break;
                default:
                    throw InternalError("Unexpected op type");
                    break;
                }
                break;
            }
            break;
        }
        if (lower.page_no == -1)
            lower = ih->leaf_begin();
        if (upper.page_no == -1)
            upper = ih->leaf_end();
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        cur_page = fh_->get_stable_page_handle(scan_->rid().page_no);
        while (!scan_->is_end())
        {
            auto rid = scan_->rid();
            auto rmd = RmRecord(fh_->get_file_hdr().record_size, cur_page->get_slot(rid.slot_no));
            if (std::all_of(fed_conds_.begin() + idx, fed_conds_.end(),
                            [&](const Condition &cond)
                            { return eval_cond(cols_, cond, rmd); }))
            {
                rid_ = scan_->rid();
                break;
            }
            else
            {
                scan_->next();
                if (is_end())
                {
                    fh_->unpin_page_handle(*cur_page);
                    return;
                }
                rid_t = scan_->rid();
                if (cur_page->page->get_page_id().page_no != rid_t.page_no)
                {
                    fh_->unpin_page_handle(*cur_page);
                    cur_page = fh_->get_stable_page_handle(rid_t.page_no);
                }
            }
        }
    }

    void nextTuple() override
    {
        scan_->next();
        if (is_end())
        {
            fh_->unpin_page_handle(*cur_page);
            return;
        }
        while (!scan_->is_end())
        {
            rid_t = scan_->rid();
            if (cur_page->page->get_page_id().page_no != rid_t.page_no)
            {
                fh_->unpin_page_handle(*cur_page);
                cur_page = fh_->get_stable_page_handle(rid_t.page_no);
            }
            auto rmd = RmRecord(fh_->get_file_hdr().record_size, cur_page->get_slot(rid_t.slot_no));
            if (std::all_of(fed_conds_.begin() + idx, fed_conds_.end(),
                            [&](const Condition &cond)
                            { return eval_cond(cols_, cond, rmd); }))
            {
                rid_ = scan_->rid();
                break;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        return std::make_unique<RmRecord>(fh_->get_file_hdr().record_size, cur_page->get_slot(rid_.slot_no));
    }

    Rid &rid() override { return rid_; }
};
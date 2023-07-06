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

class SeqScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_;             // 表的名称
    std::vector<Condition> conds_;     // scan的条件
    RmFileHandle *fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // scan后生成的记录的字段
    size_t len_;                       // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_; // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_; // table_iterator

    SmManager *sm_manager_;

public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        context_ = context;
        fed_conds_ = conds_;
    }
    const std::vector<ColMeta> &cols() const
    {
        return cols_;
    };
    bool is_end() const override { return scan_->is_end(); }
    size_t tupleLen() const { return len_; };
    std::string getType() { return "SeqScanExecutor"; };
    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, std::unique_ptr<RmRecord> &target)
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
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end())
        {
            auto rmd = fh_->get_record(scan_->rid(), context_);
            if (std::all_of(fed_conds_.begin(), fed_conds_.end(),
                            [&](const Condition &cond)
                            { return eval_cond(cols_, cond, rmd); }))
            {
                rid_ = scan_->rid();
                break;
            }
            else
                scan_->next();
        }
    }

    void nextTuple() override
    {
        assert(!is_end());
        for (scan_->next(); !scan_->is_end(); scan_->next())
        {
            auto rmd = fh_->get_record(scan_->rid(), context_);
            if (std::all_of(fed_conds_.begin(), fed_conds_.end(),
                            [&](const Condition &cond)
                            { return eval_cond(cols_, cond, rmd); }))
            {
                rid_ = scan_->rid();
                break;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        return fh_->get_record(rid_, context_);
    }
    Rid &rid() override { return rid_; }
};
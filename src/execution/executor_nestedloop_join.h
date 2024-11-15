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

class NestedLoopJoinExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
    size_t len_;                              // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;               // join后获得的记录的字段

    std::vector<Condition> fed_conds_; // join条件
    bool isend;

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds)
    {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols)
        {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    const std::vector<ColMeta> &cols() const
    {
        return cols_;
    };

    bool is_end() const override
    {
        return isend;
    };

    size_t tupleLen() const { return len_; };

    std::string getType() { return "NestedLoopJoinExecutor"; };

    void beginTuple() override
    {
        left_->beginTuple();
        right_->beginTuple();
        isend = false;
        valid_tuple();
    }

    void nextTuple() override
    {
        assert(!is_end());
        if (right_->is_end())
        {
            left_->nextTuple();
            right_->beginTuple();
        }
        else
        {
            right_->nextTuple();
        }
        valid_tuple();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        assert(!is_end());
        // 1. 取left_record和right_record
        auto left_record = left_->Next();
        auto right_record = right_->Next();
        // 2. 合并到一起
        auto ret = std::make_unique<RmRecord>(len_);
        memcpy(ret->data, left_record->data, left_record->size);
        memcpy(ret->data + left_record->size, right_record->data, right_record->size);
        return ret;
    }

    Rid &rid() override { return _abstract_rid; }

private:
    bool eval_cond(const Condition &cond)
    {
        auto left_record = left_->Next();
        auto right_record = right_->Next();
        auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
        auto left_value = get_value(left_record, *left_col);
        Value right_value;
        if (!cond.is_rhs_val)
        {
            auto right_col = *(right_->get_col(right_->cols(), cond.rhs_col));
            right_value = get_value(right_record, right_col);
        }
        else
        {
            right_value = cond.rhs_val;
        }
        return compare_value(left_value, right_value, cond.op);
    }
    void valid_tuple()
    {
        assert(!is_end());
        while (!left_->is_end())
        {
            // 取两边的record
            while (!right_->is_end())
            {
                bool is_fit = std::all_of(fed_conds_.begin(), fed_conds_.end(), [&](const Condition &cond)
                                          { return eval_cond(cond); });

                if (is_fit)
                    return;
                else
                    right_->nextTuple();
            }
            left_->nextTuple();
            right_->beginTuple();
        }
        isend = true;
    }
};
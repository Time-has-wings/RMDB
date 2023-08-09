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
#include <queue>
class NestedLoopJoinExecutor : public AbstractExecutor
{
private:
	std::unique_ptr<AbstractExecutor> left_{};  // 左儿子节点（需要join的表）
	std::unique_ptr<AbstractExecutor> right_{}; // 右儿子节点（需要join的表）
	size_t len_{};                              // join后获得的每条记录的长度
	std::vector<ColMeta> cols_{};               // join后获得的记录的字段
	std::vector<Condition> fed_conds_{};        // join条件
    int64_t join_buffer_size = 41943040;
    std::vector<std::pair<std::unique_ptr<RmRecord>, std::vector<Value>>> blocks;
	std::vector<Value> rVals{};
    int64_t now_size = 0;
	int64_t rec_size{};
	int64_t idx{};
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

	[[nodiscard]] const std::vector<ColMeta>& cols() const override
    {
        return cols_;
    };

	[[nodiscard]] bool is_end() const override
    {
        return isend;
    };

	[[nodiscard]] size_t tupleLen() const override
	{
		return len_;
	};

	std::string getType() override
	{
		return "BlockNestedLoopJoinExecutor";
	};
    void nextBlock()
    {
        now_size = 0;
        blocks.clear();
        std::vector<Value> block;
        while (!left_->is_end())
        {
            auto left_record = left_->Next();
            now_size += rec_size;
			for (const auto& cond : fed_conds_)
            {
                auto left_col = left_->get_col(left_->cols(), cond.lhs_col);
                auto left_value = get_value(*left_record, *left_col);
                block.emplace_back(left_value);
            }
			blocks.emplace_back(std::move(left_record), std::move(block));
            block.clear();
            if (now_size > join_buffer_size - rec_size)
                break;
            else
                left_->nextTuple();
        }
    }
    void beginTuple() override
    {
        left_->beginTuple();
        right_->beginTuple();
        idx = 0;
        isend = false;
        rec_size = left_->Next()->size;
        nextBlock();
        valid_tuple();
    }
    void nextTuple() override
    {
        assert(!is_end());
        valid_tuple();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        assert(idx != -1);
        auto left_record = *blocks.at(idx - 1).first;
        auto right_record = right_->Next();
        auto ret = std::make_unique<RmRecord>(len_);
        memcpy(ret->data, left_record.data, left_record.size);
        memcpy(ret->data + left_record.size, right_record->data, right_record->size);
        return ret;
    }

    Rid &rid() override { return _abstract_rid; }

private:
    bool eval_cond(const std::pair<std::unique_ptr<RmRecord>, std::vector<Value>> &block)
    {
		if (fed_conds_.empty())
            return true;
        for (int i = 0; i < fed_conds_.size(); i++)
        {
            auto cond = fed_conds_.at(i);
            if (!compare_value(block.second.at(i), rVals.at(i), cond.op))
                return false;
        }
        return true;
    }
    void valid_tuple()
    {
		while (!blocks.empty())
        {
            // 取两边的record
            while (!right_->is_end())
            {
                if (idx == 0)
                {
                    auto right_record = right_->Next();
					for (const auto& cond : fed_conds_)
                    {
                        Value right_value;
                        if (!cond.is_rhs_val)
                        {
                            auto right_col = *(right_->get_col(right_->cols(), cond.rhs_col));
                            right_value = get_value(*right_record, right_col);
                        }
                        else
                        {
                            right_value = cond.rhs_val;
                        }
                        rVals.emplace_back(right_value);
                    }
                }
                auto left_rec = std::find_if(blocks.begin() + idx, blocks.end(), [&](const std::pair<std::unique_ptr<RmRecord>, std::vector<Value>> &block)
                                             { return eval_cond(block); });

                if (left_rec != blocks.end())
                {
                    idx = left_rec - blocks.begin() + 1;
                    return;
                }
                else
                {
                    idx = 0;
                    rVals.clear();
                    right_->nextTuple();
                }
            }
            if (left_->is_end())
            {
                isend = true;
                return;
            }
            else
            {
                left_->nextTuple();
            }
            nextBlock();
            right_->beginTuple();
        }
    }
};
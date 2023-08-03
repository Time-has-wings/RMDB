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
struct value_id
{
    std::vector<std::pair<Value, bool>> v;
    std::unique_ptr<RmRecord> id;
    bool operator<(const value_id &t)
    {
        size_t s = 0;
        while (s != v.size())
        {
            if (v.at(s).first == t.v.at(s).first)
            {
                s++;
                continue;
            }
            else if (v.at(s).first < t.v.at(s).first)
                return !v.at(s).second;
            else
                return v.at(s).second;
        }
        return false;
    }
};

class SortExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_; // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    std::vector<bool> is_descs_;
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples;
    size_t idx = 0;

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<std::pair<TabCol, bool>> &orders)
    {
        prev_ = std::move(prev);
        auto &prev_cols = prev_->cols();
		for (const auto& order : orders)
        {
            auto pos = get_col(prev_cols, order.first);
            auto col = *pos;
            cols_.emplace_back(col);
            is_descs_.emplace_back(order.second);
        }
        tuple_num = 0;
        sort();
    }
	[[nodiscard]] const std::vector<ColMeta>& cols() const override
    {
        return prev_->cols();
    };
	[[nodiscard]] bool is_end() const override
    {
        return idx >tuple_num;
    }
    void beginTuple() override
    {
        idx++;
    }

    void nextTuple() override
    {
        idx++;
    }

    std::unique_ptr<RmRecord> Next() override
    {
        return std::move(sorted_tuples.at(idx-1));
    }
    void sort()
    {
        std::vector<value_id> v;
        prev_->beginTuple();
        while (!prev_->is_end())
        {
            tuple_num++;
            auto rec = prev_->Next();
            std::vector<std::pair<Value, bool>> vec;
            for (int i = 0; i < cols_.size(); i++)
            {
                auto val = prev_->get_value(*rec, cols_.at(i));
                vec.emplace_back(val, is_descs_.at(i));
            }
            v.push_back({vec, std::move(rec)});
            prev_->nextTuple();
        }
        std::sort(v.begin(), v.end());
		for (auto& i : v)
        {
			sorted_tuples.emplace_back(std::move(i.id));
        }
    }
    Rid &rid() override { return _abstract_rid; }
};
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
#include <memory>
struct value_rec
{
    std::vector<std::pair<Value, bool>> v;
    int id;
    bool operator<(value_rec t) const
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

class LimitExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_; // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    std::vector<bool> is_descs_;
    int limit;
    std::list<int> valid_tuples;
    std::unordered_map<int, std::unique_ptr<RmRecord>> s_map;

public:
    LimitExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<std::pair<TabCol, bool>> &orders, int limit_)
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
        limit = limit_;
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (!valid_tuples.empty())
        {
            auto id = valid_tuples.front();
            return std::move(s_map[id]);
        }
    }

    void init()
    {
        int id = 1;
        prev_->beginTuple();
        std::priority_queue<value_rec> tuples;
        while (!prev_->is_end())
        {
            auto rec = prev_->Next();
            auto a = *rec;
            std::vector<std::pair<Value, bool>> vec;
            for (int i = 0; i < cols_.size(); i++)
            {
                auto val = prev_->get_value(*rec, cols_.at(i));
                vec.emplace_back(val, is_descs_.at(i));
            }
            s_map[id] = std::move(rec);
            value_rec x{vec, id++};
            tuples.push(x);
            if (tuples.size() > limit)
            {
                s_map.erase(tuples.top().id);
                tuples.pop();
            }
            prev_->nextTuple();
        }
        while (!tuples.empty())
        {
            valid_tuples.push_front(tuples.top().id);
            tuples.pop();
        }
    }
	const std::vector<ColMeta>& cols() const override
    {
        return prev_->cols();
    };
    bool is_end() const override
    {
        return valid_tuples.empty();
    }
    void beginTuple() override
    {
        init();
    }

    void nextTuple() override
    {
        valid_tuples.pop_front();
    }
    Rid &rid() override { return _abstract_rid; }
};
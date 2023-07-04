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
    Value v;
    size_t id;
    bool operator<(const value_id &t)
    {
        return v < t.v;
    }
    bool operator>(const value_id &t)
    {
        return v > t.v;
    }
};

class SortExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_; // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::queue<size_t> sorted_tuples;
    std::unique_ptr<RmRecord> current_tuple;

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc)
    {
        prev_ = std::move(prev);
        auto &prev_cols = prev_->cols();
        auto pos = get_col(prev_cols, sel_cols);
        cols_ = *pos;
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
        sort();
    }
    const std::vector<ColMeta> &cols() const {
        return prev_->cols();
    };
    bool is_end() const override
    {
        return used_tuple.size() > tuple_num;
    }
    void beginTuple() override
    {
        search();
    }

    void nextTuple() override
    {
        search();
    }

    std::unique_ptr<RmRecord> Next() override
    {
        auto rec = current_tuple.get();
        auto ret = std::make_unique<RmRecord>(cols_.len);
        memcpy(ret->data, rec->data, rec->size);
        return ret;
    }
    void sort()
    {

        std::vector<value_id> v;
        size_t id = 0;
        prev_->beginTuple();
        while (!prev_->is_end())
        {
            tuple_num ++;
            auto rec = prev_->Next();
            auto val = prev_->get_value(rec, cols_);
            v.push_back({val, id++});
            prev_->nextTuple();
        }
        if (!is_desc_)
            std::sort(v.begin(), v.end());
        else
            std::sort(v.rbegin(), v.rend());
        for (auto i : v)
        {
            sorted_tuples.push(i.id);
        }
    }
    void search()
    {
        prev_->beginTuple();
        size_t id = 0;
        auto x = sorted_tuples.front();
        sorted_tuples.pop();
        used_tuple.push_back(x);
        while (!prev_->is_end())
        {
            if (id++ != x)
            {
                prev_->nextTuple();
                continue;
            }
            else
            {
                current_tuple = prev_->Next();
                break;
            }
        }
    }
    Rid &rid() override { return _abstract_rid; }
};
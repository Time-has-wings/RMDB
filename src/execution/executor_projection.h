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

class ProjectionExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_; // 投影节点的儿子节点
    std::vector<ColMeta> cols_;              // 需要投影的字段
    size_t len_;                             // 字段总长度
    std::vector<size_t> sel_idxs_;

public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols)
    {
        prev_ = std::move(prev);
        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols)
        {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.emplace_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.emplace_back(col);
        }
        len_ = curr_offset;
    }

    size_t tupleLen() const { return len_; };

    std::string getType() { return "ProjectionExecutor"; };


    void beginTuple() override {
        prev_->beginTuple();
    }
    void nextTuple() override {
        prev_->nextTuple();
    }

    const std::vector<ColMeta> &cols() const {
        return cols_;
    };

    // 投影操作is_end()和子节点的is_end()相同
    bool is_end() const override { 
        return prev_->is_end(); 
    };

    std::unique_ptr<RmRecord> Next() override {
        auto prev_record = prev_->Next();
        auto ret = std::make_unique<RmRecord>(len_);
        auto &prev_cols = prev_->cols();
        for(size_t i = 0; i < sel_idxs_.size(); i++) {
            auto sel_idx = sel_idxs_[i];
            auto &pre_col = prev_cols[sel_idx];
            auto &sel_col = cols_[i];
            memcpy(ret->data + sel_col.offset, prev_record->data + pre_col.offset, sel_col.len);
        }
        return ret;
    }


    Rid &rid() override { return _abstract_rid; }
};
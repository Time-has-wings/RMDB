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

class InsertExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle *fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
	Rid rid_{};                   // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;
    std::vector<RmRecord> s;

public:
	InsertExecutor(SmManager* sm_manager,
		const std::string& tab_name,
		const std::vector<Value>& values,
		Context* context)
    {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size())
        {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override
    {
        if (context_->txn_->get_txn_mode())  // 事务模式下
        {
            bool res = (context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd()));
			if (!res)
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
        RmRecord rec(fh_->get_file_hdr().record_size);
        // 合法性检查
        for (size_t i = 0; i < values_.size(); i++)
        {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            val.value_change(col.type);
            if (col.type != val.type)
            {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        for (size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto &index = tab_.indexes[i];
            s.emplace_back(index.col_tot_len);
			auto ih = sm_manager_->ihs_.at(IxManager::get_index_name(tab_name_, index.cols)).get();
            int offset = 0;
            for (size_t j = 0; j < index.col_num; ++j)
            {
                memcpy(s[i].data + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            if (ih->get_value(s[i].data, nullptr, context_->txn_))
                throw IndexEnrtyExistsError();
        }
        // record文件插入记录
        rid_ = fh_->insert_record(rec.data, context_);
        // 事务写操作
		auto* wrec = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_);
        context_->txn_->append_write_record(wrec);
        // Insert日志
        InsertLogRecord insert_log(context_->txn_->get_transaction_id(), rec, rid_, tab_name_);
        insert_log.prev_lsn_ = context_->txn_->get_prev_lsn();
        context_->log_mgr_->add_log_to_buffer(&insert_log);
        context_->txn_->set_prev_lsn(insert_log.lsn_);
        // index文件更新
        for (size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto &index = tab_.indexes[i];
			auto ih = sm_manager_->ihs_.at(IxManager::get_index_name(tab_name_, index.cols)).get();
            ih->insert_entry(s[i].data, rid_, context_->txn_);
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};
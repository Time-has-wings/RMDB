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
	RmFileHandle* fh_;                 // 表的数据文件句柄
	std::vector<ColMeta> cols_;        // scan后生成的记录的字段
	size_t len_;                       // scan后生成的每条记录的长度
	std::vector<Condition> fed_conds_; // 同conds_，两个字段相同

	Rid rid_{};
	Rid rid_t{};
	std::unique_ptr<RecScan> scan_; // table_iterator
	std::shared_ptr<RmPageHandle> cur_page;
	std::vector<std::vector<ColMeta>::const_iterator> l_cols;
	Value l_val, r_val;
	SmManager* sm_manager_;
	std::shared_ptr<RmRecord> rmd;
	SQLType sql_type_;                  // 判断对表上S锁还是IX锁还是X锁(select->S, delete->X, update->IX)

 public:
	SeqScanExecutor(SmManager* sm_manager, std::string tab_name, std::vector<Condition> conds, Context* context, SQLType sql_type)
	{
		sm_manager_ = sm_manager;
		tab_name_ = std::move(tab_name);
		conds_ = std::move(conds);
		TabMeta& tab = sm_manager_->db_.get_table(tab_name_);
		fh_ = sm_manager_->fhs_.at(tab_name_).get();
		cols_ = tab.cols;
		len_ = fh_->get_file_hdr().record_size;
		context_ = context;
		fed_conds_ = conds_;
		for (auto& cond : conds_)
		{
			l_cols.emplace_back(get_col(cols_, cond.lhs_col));
		}
		rmd = std::make_shared<RmRecord>(len_);
		sql_type_= sql_type;
	}
	[[nodiscard]] const std::vector<ColMeta>& cols() const override
	{
		return cols_;
	};
	[[nodiscard]] bool is_end() const override
	{
		return scan_->is_end();
	}
	[[nodiscard]] size_t tupleLen() const override
	{
		return len_;
	};
	std::string getType() override
	{
		return "SeqScanExecutor";
	};
	bool eval_conds(const RmRecord& target)
	{
		bool flag = true;
		for (size_t i = 0; i < conds_.size(); i++)
		{
			auto& lhs_col = l_cols[i];
			auto& cond = conds_[i];
			set_value(target, (*lhs_col), l_val);
			if (!cond.is_rhs_val)
			{
				auto rhs_col = get_col(cols_, cond.rhs_col);
				set_value(target, *rhs_col, r_val);
				if (!compare_value(l_val, r_val, cond.op))
				{
					flag = false;
					break;
				}
			}
			else
			{
				if (!compare_value(l_val, cond.rhs_val, cond.op))
				{
					flag = false;
					break;
				}
			}
		}
		return flag;
	}

	void beginTuple() override
	{
		if (context_->txn_->get_txn_mode()) // 事务模式下
		{
			if (sql_type_ == SQLType::select_sql) // 对表上S锁
			{
				bool res = context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
				if (!res)
					throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
			}
			// else if (sql_type_ == SQLType::delete_sql) // 对表上X锁
			// {
			// 	bool res = context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
			// 	if (!res)
			// 		throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
			// }
			// else if (sql_type_ == SQLType::delete_sql) // 对表上IX锁
			// {
			// 	bool res = context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
			// 	if (!res)
			// 		throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
			// }
			// else if (sql_type_ == SQLType::update_sql) // 对表上IX锁
			// {
			// 	bool res = context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
			// 	if (!res)
			// 		throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
			// }
			else; // 不存在此情形
		}
		scan_ = std::make_unique<RmScan>(fh_);
		cur_page = fh_->get_stable_page_handle(scan_->rid().page_no);
		while (!scan_->is_end())
		{
			rid_t = scan_->rid();
			if (cur_page->page->get_page_id().page_no != rid_t.page_no)
			{
				fh_->unpin_page_handle(*cur_page, false);
				cur_page = fh_->get_stable_page_handle(rid_t.page_no);
			}
			rmd->SetData(cur_page->get_slot(rid_t.slot_no));
			if (eval_conds(*rmd))
			{
				rid_ = rid_t;
				break;
			}
			else
			{
				scan_->next();
			}
		}
		if (is_end())
		{
			fh_->unpin_page_handle(*cur_page, false);
		}
	}
	void nextTuple() override
	{
		scan_->next();
		if (is_end())
		{
			fh_->unpin_page_handle(*cur_page, false);
			return;
		}
		while (!scan_->is_end())
		{
			rid_t = scan_->rid();
			if (cur_page->page->get_page_id().page_no != rid_t.page_no)
			{
				fh_->unpin_page_handle(*cur_page, false);
				cur_page = fh_->get_stable_page_handle(rid_t.page_no);
			}
			rmd->SetData(cur_page->get_slot(rid_t.slot_no));
			if (eval_conds(*rmd))
			{
				rid_ = scan_->rid();
				return;
			}
			scan_->next();
		}
		if (is_end())
		{
			fh_->unpin_page_handle(*cur_page, false);
		}
	}
	std::unique_ptr<RmRecord> Next() override
	{
		return std::make_unique<RmRecord>(len_, cur_page->get_slot(rid_.slot_no));
	}
	Rid& rid() override
	{
		return rid_;
	}
	ColMeta get_col_offset(const TabCol& target) override
	{
		return *get_col(cols_, target);
	};
};
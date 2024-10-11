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
#include <utility>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor
{
 private:
	TabMeta tab_;
	std::vector<Condition> conds_;
	RmFileHandle* fh_;
	std::vector<Rid> rids_;
	std::string tab_name_;
	std::vector<SetClause> set_clauses_;
	SmManager* sm_manager_;
	std::vector<IndexMeta> index_queue;
	std::vector<ColMeta> col_queue;
	std::vector<RmRecord> rm_vector;
	int s;

 public:
	UpdateExecutor(SmManager* sm_manager, const std::string& tab_name, std::vector<SetClause> set_clauses,
		std::vector<Condition> conds, std::vector<Rid> rids, Context* context)
	{
		sm_manager_ = sm_manager;
		tab_name_ = tab_name;
		set_clauses_ = std::move(set_clauses);
		tab_ = sm_manager_->db_.get_table(tab_name);
		fh_ = sm_manager_->fhs_.at(tab_name).get();
		conds_ = std::move(conds);
		rids_ = std::move(rids);
		context_ = context;
		s = rids_.size();
	}
	std::unique_ptr<RmRecord> Next() override
	{
		// if (context_->txn_->get_txn_mode())  // 事务模式下
		// {
		// 	bool res = (context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd()));
		// 	if (!res)
		// 		throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
		// }
		if (context_->txn_->get_txn_mode()) // 事务模式下
		{
			bool res = context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
			if (!res)
				throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
			for (auto rid : rids_)
			{
				res = context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fh_->GetFd());
				if (!res)
			 		throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
			}
		}
		// if (context_->txn_->get_txn_mode())
		// {
		// 	bool res = context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
		// 	if (!res)
		// 		throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
		// }
		// 合法性检查
		for (auto& clause : set_clauses_)
		{
			auto col = tab_.get_col(clause.lhs.col_name);
			col_queue.emplace_back(*col);
			clause.rhs.value_change(col->type);
			if (clause.rhs.type != col->type)
			{
				throw IncompatibleTypeError(coltype2str(col->type), coltype2str(clause.rhs.type));
			}
			clause.rhs.init_raw(col->len);
			if (clause.is_update_col && clause.op == ast::SV_OP_SUB)
			{
				switch (clause.rhs.type)
				{
				case TYPE_INT:
					*(int*)clause.rhs.raw->data = -*(int*)clause.rhs.raw->data;
					break;
				case TYPE_FLOAT:
					*(float*)clause.rhs.raw->data = -*(float*)clause.rhs.raw->data;
					break;
				}
			}
		}
		for (auto& rid : rids_)
		{
			auto rec = *(fh_->get_record(rid, context_));
			for (auto& clause : set_clauses_)
			{
				auto col = tab_.get_col(clause.lhs.col_name);
				if (clause.is_update_col)
				{
					switch (col->type)
					{
					case TYPE_INT:
						*(int*)(rec.data + col->offset) = *(int*)(rec.data + col->offset) + *(int*)clause.rhs.raw->data;
						break;
					case TYPE_FLOAT:
						*(float*)(rec.data + col->offset) =
							*(float*)(rec.data + col->offset) + *(float*)clause.rhs.raw->data;
						break;
					default:
						break;
					}
				}
				else
					memcpy(rec.data + col->offset, clause.rhs.raw->data, col->len);
			}
			rm_vector.emplace_back(rec);
		}
		for (auto& index : tab_.indexes)
		{
			for (auto& col : col_queue)
			{
				if (std::any_of(index.cols.begin(), index.cols.end(), [&col](const ColMeta& c)
				{ return col.name == c.name; }))
				{
					index_queue.emplace_back(index);
					break;
				}
			}
		}
		for (auto& index : index_queue)
		{
			char key_old[index.col_tot_len];
			char key[index.col_tot_len];
			for (int i = 0; i < s - 1; i++)
			{
				auto& rec_i = rm_vector[i];
				int offset_i = 0;
				for (int k = 0; k < index.col_num; ++k)
				{
					memcpy(key_old + offset_i, rec_i.data + index.cols[k].offset, index.cols[k].len);
					offset_i += index.cols[k].len;
				}
				for (int j = i + 1; j < s; j++)
				{
					auto& rec_j = rm_vector[j];
					int offset_j = 0;
					for (int k = 0; k < index.col_num; ++k)
					{
						memcpy(key + offset_j, rec_j.data + index.cols[k].offset, index.cols[k].len);
						offset_j += index.cols[k].len;
					}
					if (memcmp(key_old, key, index.col_tot_len) == 0)
						throw IndexEnrtyExistsError();
				}
			}
		}
		for (int i = 0; i < s; i++)
		{
			auto& update_record = rm_vector[i];
			auto& rid = rids_[i];
			std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
			// Update日志
			UpdateLogRecord update_log(context_->txn_->get_transaction_id(), *(rec), update_record, rid, tab_name_);
			update_log.prev_lsn_ = context_->txn_->get_prev_lsn();
			context_->log_mgr_->add_log_to_buffer(&update_log);
			context_->txn_->set_prev_lsn(update_log.lsn_);
			// index文件更新
			for (auto& index : index_queue)
			{
				auto ihs =
					sm_manager_->ihs_.at(IxManager::get_index_name(tab_.name, index.cols)).get();
				char update[index.col_tot_len];
				char orign[index.col_tot_len];
				int offset = 0;
				for (int j = 0; j < index.col_num; ++j)
				{
					memcpy(update + offset, update_record.data + index.cols[j].offset, index.cols[j].len);
					memcpy(orign + offset, rec->data + index.cols[j].offset, index.cols[j].len);
					offset += index.cols[j].len;
				}
				if (memcmp(update, orign, index.col_tot_len) == 0)
					continue;
				else if (ihs->get_value(update, nullptr, context_->txn_))
					throw IndexEnrtyExistsError();
				else
				{
					ihs->delete_entry(orign, context_->txn_);
					ihs->insert_entry(update, rid, context_->txn_);
				}
			}
			// record文件update
			fh_->update_record(rid, update_record.data, context_);
			// 事务写操作
			auto* wrec = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec);
			context_->txn_->append_write_record(wrec);
		}
		return nullptr;
	}
	Rid& rid() override
	{
		return _abstract_rid;
	}
};
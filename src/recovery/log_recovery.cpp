/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
		http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze()
{
	int off_set = 0;
	redo_lsn = INT32_MAX;
	disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), 0);
	auto t = std::make_shared<LogRecord>();
	t->deserialize(buffer_.buffer_ + off_set);
	while (t->log_tot_len_ != 0)
	{
		if (t->log_type_ == begin)
		{
			ATT.push_back(std::pair<txn_id_t, lsn_t>{t->log_tid_, t->lsn_});
		}
		else if (t->log_type_ == commit)
		{
			ATT.erase(std::find_if(ATT.begin(), ATT.end(), [t](const std::pair<txn_id_t, lsn_t> &s)
								   { return s.first == t->log_tid_; }));
		}
		else if (t->log_type_ == INSERT)
		{
			InsertLogRecord temp;
			temp.deserialize(buffer_.buffer_ + off_set);
			redo_lsn = redo_lsn < temp.lsn_ ? redo_lsn : temp.lsn_;
			if (std::find_if(DPT.begin(), DPT.end(), [temp](const page_id_t &s)
							 { return s == temp.rid_.page_no; }) != DPT.end())
				DPT.push_back(temp.rid_.page_no);
			auto t_id = std::find_if(ATT.begin(), ATT.end(), [temp](const std::pair<txn_id_t, lsn_t> &s)
									 { return s.first == temp.log_tid_; });
			if (t_id != ATT.end())
				t_id->second = t->lsn_;
		}
		else if (t->log_type_ == DELETE)
		{
			DeleteLogRecord temp;
			temp.deserialize(buffer_.buffer_ + off_set);
			redo_lsn = redo_lsn < temp.lsn_ ? redo_lsn : temp.lsn_;
			if (std::find_if(DPT.begin(), DPT.end(), [temp](const page_id_t &s)
							 { return s == temp.rid_.page_no; }) != DPT.end())
				DPT.push_back(temp.rid_.page_no);
			auto t_id = std::find_if(ATT.begin(), ATT.end(), [temp](const std::pair<txn_id_t, lsn_t> &s)
									 { return s.first == temp.log_tid_; });
			if (t_id != ATT.end())
				t_id->second = t->lsn_;
		}
		else if (t->log_type_ == UPDATE)
		{
			UpdateLogRecord temp;
			temp.deserialize(buffer_.buffer_ + off_set);
			redo_lsn = redo_lsn < temp.lsn_ ? redo_lsn : temp.lsn_;
			if (std::find_if(DPT.begin(), DPT.end(), [temp](const page_id_t &s)
							 { return s == temp.rid_.page_no; }) != DPT.end())
				DPT.push_back(temp.rid_.page_no);
			auto t_id = std::find_if(ATT.begin(), ATT.end(), [temp](const std::pair<txn_id_t, lsn_t> &s)
									 { return s.first == temp.log_tid_; });
			if (t_id != ATT.end())
				t_id->second = t->lsn_;
		}
		off_set += t->log_tot_len_;
		t->deserialize(buffer_.buffer_ + off_set);
	}
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo()
{
	int off_set = 0;
	disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), 0);
	auto t = std::make_shared<LogRecord>();
	t->deserialize(buffer_.buffer_ + off_set);
	while (t->log_tot_len_ != 0)
	{
		if (t->lsn_ < redo_lsn)
		{
			off_set += t->log_tot_len_;
			t->deserialize(buffer_.buffer_ + off_set);
			continue;
		}
		if (t->log_type_ == INSERT)
		{
			InsertLogRecord temp;
			temp.deserialize(buffer_.buffer_ + off_set);
			char tab_name[temp.table_name_size_ + 1];
			memset(tab_name, '\0', temp.table_name_size_ + 1);
			memcpy(tab_name, temp.table_name_, temp.table_name_size_);
			auto fh_ = sm_manager_->fhs_.at(tab_name).get();
			fh_->insert_record(temp.insert_value_.data, nullptr);
		}
		else if (t->log_type_ == DELETE)
		{
			DeleteLogRecord temp;
			temp.deserialize(buffer_.buffer_ + off_set);
			char tab_name[temp.table_name_size_];
			memset(tab_name, '\0', temp.table_name_size_ + 1);
			memcpy(tab_name, temp.table_name_, temp.table_name_size_);
			auto fh_ = sm_manager_->fhs_.at(tab_name).get();
			fh_->delete_record(temp.rid_, nullptr);
		}
		else if (t->log_type_ == UPDATE)
		{
			UpdateLogRecord temp;
			temp.deserialize(buffer_.buffer_ + off_set);
			char tab_name[temp.table_name_size_];
			memset(tab_name, '\0', temp.table_name_size_ + 1);
			memcpy(tab_name, temp.table_name_, temp.table_name_size_);
			auto fh_ = sm_manager_->fhs_.at(tab_name).get();
			fh_->update_record(temp.rid_, temp.update_value_.data, nullptr);
		}
		off_set += t->log_tot_len_;
		t->deserialize(buffer_.buffer_ + off_set);
	}
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo()
{
	for (auto &att : ATT)
	{
		auto &lsn = att.second;
		auto t = std::make_shared<LogRecord>();
		t->deserialize(buffer_.buffer_ + lsn);
		while ((t->prev_lsn_ != -1))
		{
			if (t->log_type_ == INSERT)
			{
				InsertLogRecord temp;
				temp.deserialize(buffer_.buffer_ + lsn);
				char tab_name[temp.table_name_size_ + 1];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				auto fh_ = sm_manager_->fhs_.at(tab_name).get();
				fh_->delete_record(temp.rid_, nullptr);
			}
			else if (t->log_type_ == DELETE)
			{
				DeleteLogRecord temp;
				temp.deserialize(buffer_.buffer_ + lsn);
				char tab_name[temp.table_name_size_];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				auto fh_ = sm_manager_->fhs_.at(tab_name).get();
				fh_->insert_record(temp.rid_, temp.delete_value_.data);
			}
			else if (t->log_type_ == UPDATE)
			{
				UpdateLogRecord temp;
				temp.deserialize(buffer_.buffer_ + lsn);
				char tab_name[temp.table_name_size_];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				auto fh_ = sm_manager_->fhs_.at(tab_name).get();
				fh_->update_record(temp.rid_, temp.orign_value_.data, nullptr);
			}
			lsn = t->prev_lsn_;
			t->deserialize(buffer_.buffer_ + lsn);
		}
	}
}
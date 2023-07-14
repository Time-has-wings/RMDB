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
	int log_file_size = disk_manager_->get_file_size(LOG_FILE_NAME); //日志文件大小
	int log_length_min = (int)(sizeof(LogType) + sizeof(lsn_t) + sizeof(uint32_t) + sizeof(txn_id_t) + sizeof(lsn_t)); //最短的记录的长度
	int buffer_size = sizeof(buffer_); //buffer_大小
	int read_log_offset = 0; //仍旧按照一整个buffer_的大小进行读取,但是偏移量不再以page_id*sizeof(buffer_)为标准
	int readbytes = disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), read_log_offset);
	while (readbytes > 0)
	{
		int off_set = 0; //在本buffer的偏移
		int read_real_size = std::min(buffer_size, log_file_size - read_log_offset); //read_log实际读入的大小
		auto t = std::make_shared<LogRecord>();
		while (true) {
			t->deserialize(buffer_.buffer_ + off_set);
			if (off_set + t->log_tot_len_ > read_real_size) //日志跨缓冲区了
				break;
			if (t->log_type_ == begin)
			{
				ATT.push_back(std::pair<txn_id_t, lsn_t>{t->log_tid_, t->lsn_});
			}
			else if (t->log_type_ == commit) //abort也是需要undo
			{
				ATT.erase(std::find_if(ATT.begin(), ATT.end(), [t](const std::pair<txn_id_t, lsn_t> &s)
									   { return s.first == t->log_tid_; }));
			}
			else if (t->log_type_ == INSERT)
			{
				InsertLogRecord temp;
				temp.deserialize(buffer_.buffer_ + off_set);
				redo_lsn = redo_lsn < temp.lsn_ ? redo_lsn : temp.lsn_; //redo_lsn最开始在定义时便有了最大值 下同
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
			if (off_set + log_length_min >= buffer_size) //取等时,表明刚好读完这个缓冲区
				break; 									//不取等时,表明最后的一条日志可能会丢失一些字段(尤其是log_tot_len_字段)
		}
		read_log_offset += off_set;
		readbytes = disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), read_log_offset);
	}
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo()
{
	int log_file_size = disk_manager_->get_file_size(LOG_FILE_NAME); //日志文件大小
	int log_length_min = (int)(sizeof(LogType) + sizeof(lsn_t) + sizeof(uint32_t) + sizeof(txn_id_t) + sizeof(lsn_t)); //最短的记录的长度
	int buffer_size = sizeof(buffer_); //buffer_大小
	int read_log_offset = 0; //仍旧按照一整个buffer_的大小进行读取,但是偏移量不再以page_id*sizeof(buffer_)为标准
	int readbytes = disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), read_log_offset);
	while (readbytes > 0)
	{
		int off_set = 0; //在本buffer的偏移
		int read_real_size = std::min(buffer_size, log_file_size - read_log_offset); //read_log实际读入的大小
		auto t = std::make_shared<LogRecord>();
		while (true) {
			t->deserialize(buffer_.buffer_ + off_set);
			if (off_set + t->log_tot_len_ > read_real_size) //日志跨缓冲区了
				break;
			if (t->lsn_ < redo_lsn)
			{
	 			off_set += t->log_tot_len_;
	 			continue;
	 		}
	 		if (t->log_type_ == INSERT)
			{
				InsertLogRecord temp;
				temp.deserialize(buffer_.buffer_ + off_set);
				char tab_name[temp.table_name_size_ + 1];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				insert_record(tab_name, temp.insert_value_.data, temp.rid_);
			}
			else if (t->log_type_ == DELETE)
			{
				DeleteLogRecord temp;
				temp.deserialize(buffer_.buffer_ + off_set);
				char tab_name[temp.table_name_size_ + 1];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				delete_record(tab_name, temp.rid_);
			}
			else if (t->log_type_ == UPDATE)
			{
				UpdateLogRecord temp;
				temp.deserialize(buffer_.buffer_ + off_set);
				char tab_name[temp.table_name_size_ + 1];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				update_record(tab_name, temp.update_value_.data, temp.rid_);
			}
			off_set += t->log_tot_len_;
			if (off_set + log_length_min >= buffer_size) //取等时,表明刚好读完这个缓冲区
				break; 									//不取等时,表明最后的一条日志可能会丢失一些字段(尤其是log_tot_len_字段)
		}
		read_log_offset += off_set;
		readbytes = disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), read_log_offset);
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
		int readbytes = disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), lsn); //读一条日志就需要调用一次read_log,同时因为不知道一条log的长度有多大,只好以一个缓冲区大小读出来
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
				delete_record(tab_name, temp.rid_);
			}
			else if (t->log_type_ == DELETE)
			{
				DeleteLogRecord temp;
				temp.deserialize(buffer_.buffer_ + lsn);
				char tab_name[temp.table_name_size_ + 1];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				insert_record(tab_name, temp.delete_value_.data, temp.rid_);
			}
			else if (t->log_type_ == UPDATE)
			{
				UpdateLogRecord temp;
				temp.deserialize(buffer_.buffer_ + lsn);
				char tab_name[temp.table_name_size_ + 1];
				memset(tab_name, '\0', temp.table_name_size_ + 1);
				memcpy(tab_name, temp.table_name_, temp.table_name_size_);
				update_record(tab_name, temp.orign_value_.data, temp.rid_);
			}
			lsn = t->prev_lsn_;
			readbytes = disk_manager_->read_log(buffer_.buffer_, sizeof(buffer_), lsn); //同上
			t->deserialize(buffer_.buffer_ + lsn);
		}
	}
}
void RecoveryManager::insert_record(char *tab_name, char *buf, Rid &rid)
{
	auto &tab_ = sm_manager_->db_.get_table(tab_name);
	auto fh_ = sm_manager_->fhs_.at(tab_name).get();
	fh_->insert_record(rid, buf);
	for (size_t i = 0; i < tab_.indexes.size(); ++i)
	{
		auto &index = tab_.indexes[i];
		auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
		char key[index.col_tot_len];
		int offset = 0;
		for (size_t i = 0; i < index.col_num; ++i)
		{
			memcpy(key + offset, buf + index.cols[i].offset, index.cols[i].len);
			offset += index.cols[i].len;
		}
		ih->insert_entry(key, rid, nullptr);
	}
}
void RecoveryManager::update_record(char *tab_name, char *buf, Rid &rid)
{
	auto &tab_ = sm_manager_->db_.get_table(tab_name);
	auto fh_ = sm_manager_->fhs_.at(tab_name).get();
	auto rec = fh_->get_record(rid, nullptr);
	for (auto &index : tab_.indexes)
	{
		auto ihs = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
		char *update = new char[index.col_tot_len];
		char *orign = new char[index.col_tot_len];
		int offset = 0;
		for (size_t i = 0; i < index.col_num; ++i)
		{
			memcpy(update + offset, buf + index.cols[i].offset, index.cols[i].len);
			memcpy(orign + offset, rec->data + index.cols[i].offset, index.cols[i].len);
			offset += index.cols[i].len;
		}
		ihs->delete_entry(orign, nullptr);
		ihs->insert_entry(update, rid, nullptr);
	}
	fh_->update_record(rid, buf, nullptr);
}
void RecoveryManager::delete_record(char *tab_name, Rid &rid)
{
	auto &tab_ = sm_manager_->db_.get_table(tab_name);
	auto fh_ = sm_manager_->fhs_.at(tab_name).get();
	auto rec = fh_->get_record(rid, nullptr);
	for (size_t i = 0; i < tab_.indexes.size(); ++i)
	{
		auto &index = tab_.indexes[i];
		auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
		char key[index.col_tot_len];
		int offset = 0;
		for (size_t i = 0; i < index.col_num; ++i)
		{
			memcpy(key + offset, rec->data + index.cols[i].offset, index.cols[i].len);
			offset += index.cols[i].len;
		}
		ih->delete_entry(key, nullptr);
	}
	fh_->delete_record(rid, nullptr);
}
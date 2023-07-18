/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
		http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "common/common.h"
#include "errors.h"
#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"
#include "sourcelib/LoadData.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string &db_name)
{
	struct stat st;
	return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name)
{
	if (is_dir(db_name))
	{
		throw DatabaseExistsError(db_name);
	}
	// 为数据库创建一个子目录
	std::string cmd = "mkdir " + db_name;
	if (system(cmd.c_str()) < 0)
	{ // 创建一个名为db_name的目录
		throw UnixError();
	}
	if (chdir(db_name.c_str()) < 0)
	{ // 进入名为db_name的目录
		throw UnixError();
	}
	// 创建系统目录
	DbMeta *new_db = new DbMeta();
	new_db->name_ = db_name;

	// 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
	std::ofstream ofs(DB_META_NAME);

	// 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
	ofs << *new_db; // 注意：此处重载了操作符<<

	delete new_db;

	// 创建日志文件
	disk_manager_->create_file(LOG_FILE_NAME);

	// 回到根目录
	if (chdir("..") < 0)
	{
		throw UnixError();
	}
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name)
{
	if (!is_dir(db_name))
	{
		throw DatabaseNotFoundError(db_name);
	}
	std::string cmd = "rm -r " + db_name;
	if (system(cmd.c_str()) < 0)
	{
		throw UnixError();
	}
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name)
{
	if (!is_dir(db_name))
		throw DatabaseNotFoundError(db_name);
	if (chdir(db_name.c_str()) == -1)
		throw UnixError();
	std::ifstream ifs(DB_META_NAME);
	ifs >> db_;
	for (auto &itab : db_.tabs_)
	{
		auto &tab = itab.second;
		fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));
		for (auto index : tab.indexes)
		{
			auto index_name = ix_manager_->get_index_name(tab.name, index.cols);
			assert(ihs_.count(index_name) == 0);
			ihs_.emplace(index_name, ix_manager_->open_index(tab.name, index.cols));
		}
	}
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta()
{
	// 默认清空文件
	std::ofstream ofs(DB_META_NAME);
	ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db()
{
	std::ofstream ofs(DB_META_NAME);
	ofs << db_;
	db_.name_.clear();
	db_.tabs_.clear();
	for (auto &entry : fhs_)
		rm_manager_->close_file(entry.second.get());
	for (auto &entry : ihs_)
		ix_manager_->close_index(entry.second.get());
	fhs_.clear();
	ihs_.clear();
	if (chdir("..") < 0)
		throw UnixError();
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context *context)
{
	std::fstream outfile;
	if (outputfile)
	{
		outfile.open("output.txt", std::ios::out | std::ios::app);
		outfile << "| Tables |\n";
	}
	RecordPrinter printer(1);
	printer.print_separator(context);
	printer.print_record({"Tables"}, context);
	printer.print_separator(context);
	for (auto &entry : db_.tabs_)
	{
		auto &tab = entry.second;
		printer.print_record({tab.name}, context);
		if (outfile)
			outfile << "| " << tab.name << " |\n";
	}
	printer.print_separator(context);
	if (outfile)
		outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string &tab_name, Context *context)
{
	TabMeta &tab = db_.get_table(tab_name);

	std::vector<std::string> captions = {"Field", "Type", "Index"};
	RecordPrinter printer(captions.size());
	// Print header
	printer.print_separator(context);
	printer.print_record(captions, context);
	printer.print_separator(context);
	// Print fields
	for (auto &col : tab.cols)
	{
		std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
		printer.print_record(field_info, context);
	}
	// Print footer
	printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context)
{
	if (db_.is_table(tab_name))
	{
		throw TableExistsError(tab_name);
	}
	// Create table meta
	int curr_offset = 0;
	TabMeta tab;
	tab.name = tab_name;
	for (auto &col_def : col_defs)
	{
		ColMeta col = {.tab_name = tab_name,
					   .name = col_def.name,
					   .type = col_def.type,
					   .len = col_def.len,
					   .offset = curr_offset,
					   .index = false};
		curr_offset += col_def.len;
		tab.cols.push_back(col);
	}
	int record_size = curr_offset;
	rm_manager_->create_file(tab_name, record_size);
	db_.tabs_[tab_name] = tab;
	fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
	flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string &tab_name, Context *context)
{
	TabMeta &tab = db_.get_table(tab_name);
	for (auto &index : tab.indexes)
	{
		drop_index(tab_name, index.cols, context);
	}
	tab.indexes.clear();
	rm_manager_->close_file(fhs_.at(tab_name).get());
	rm_manager_->destroy_file(tab_name);
	db_.tabs_.erase(tab_name);
	fhs_.erase(tab_name);
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
	// my update
	int col_sum_len = 0;
	TabMeta &tab = db_.get_table(tab_name);
	if (tab.is_index(col_names))
	{
		throw IndexExistsError(tab_name, col_names);
	}
	IndexMeta index_tab;
	for (auto &col_name : col_names)
	{
		auto col = tab.get_col(col_name);
		col->index = true;
		index_tab.cols.push_back(*col);
		col_sum_len += col->len;
	}
	index_tab.tab_name = tab_name;
	index_tab.col_num = col_names.size();
	index_tab.col_tot_len = col_sum_len;
	ix_manager_->create_index(tab_name, index_tab.cols);
	auto ih = ix_manager_->open_index(tab_name, index_tab.cols);
	auto file_handle = fhs_.at(tab_name).get();
	for (RmScan rm_scan(file_handle); !rm_scan.is_end(); rm_scan.next())
	{
		auto rec = file_handle->get_record(rm_scan.rid(), context);
		char key[index_tab.col_tot_len];
		char *data = rec->data;
		int offset = 0;
		for (size_t i = 0; i < index_tab.cols.size(); i++)
		{
			std::memcpy(key + offset, data + index_tab.cols[i].offset, index_tab.cols[i].len);
			offset += index_tab.cols[i].len;
		}
		ih->insert_entry(key, rm_scan.rid(), context->txn_);
	}
	auto index_name = ix_manager_->get_index_name(tab_name, index_tab.cols);
	assert(ihs_.count(index_name) == 0);
	ihs_.emplace(index_name, std::move(ih));
	tab.indexes.push_back(index_tab);
	flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{
	TabMeta &tab = db_.tabs_[tab_name];
	if (!tab.is_index(col_names))
	{
		throw IndexNotFoundError(tab_name, col_names);
	}
	for (auto &col_name : col_names)
	{
		auto col = tab.get_col(col_name);
		col->index = false;
	}
	auto s = tab.get_index_meta(col_names);
	tab.indexes.erase(s);
	auto index_name = ix_manager_->get_index_name(tab_name, col_names);
	ix_manager_->close_index(ihs_.at(index_name).get());
	ix_manager_->destroy_index(tab_name, col_names);
	ihs_.at(index_name).~unique_ptr();
	ihs_.erase(index_name);
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<ColMeta> &cols, Context *context)
{
	std::vector<std::string> col_names;
	for (auto &col : cols)
	{
		col_names.push_back(col.name);
	}
	drop_index(tab_name, col_names, context);
}
void SmManager::show_indexes(const std::string &tab_name, Context *context)
{
	std::fstream outfile;
	if (outputfile)
		outfile.open("output.txt", std::ios::out | std::ios::app);
	TabMeta &tab = db_.get_table(tab_name);
	// if (!tab.)
	for (auto &index : tab.indexes)
	{
		if (outputfile)
		{
			outfile << "| " << tab.name << " | "
					<< "unique"
					<< " | (";
			int i = 0;
			for (auto &col : index.cols)
			{
				if (i + 1 == index.cols.size())
					outfile << col.name;
				else
					outfile << col.name << ",";
				i++;
			}
			outfile << ") |\n";
		}
		RecordPrinter printer(1);
		printer.print_separator(context);
		printer.print_record({tab_name}, context);
		printer.print_separator(context);
		printer.print_record_index(index.cols, context);

		printer.print_separator(context);
	}
	if (outfile)
		outfile.close();
}

void SmManager::rollback_delete(const std::string &tab_name,
								const Rid &tuple_rid,
								const RmRecord &record,
								Context *context)
{
	auto tab = db_.get_table(tab_name); // 获取表元数据
	// 索引文件插入key
	for (IndexMeta &index : tab.indexes)
	{
		std::vector<ColMeta> index_cols = index.cols;									 // 索引包含的字段
		std::string index_file_name = ix_manager_->get_index_name(tab_name, index_cols); // 获得索引所在文件名
		char *key = new char[index.col_tot_len];										 // 索引字段下的key
		char *data = record.data;														 // 这条记录的序列化数据
		int offset = 0;
		for (size_t i = 0; i < index_cols.size(); i++)
		{ // 获取record下的key
			std::memcpy(key + offset, data + index_cols[i].offset, index_cols[i].len);
			offset += index_cols[i].len;
		}
		ihs_.at(index_file_name).get()->insert_entry(key, tuple_rid, context->txn_); // 删除
	}
	// 表文件插入记录
	// fhs_.at(tab_name).get()->insert_record(record.data, context);
	fhs_.at(tab_name).get()->insert_record(tuple_rid, record.data);
}

void SmManager::rollback_insert(const std::string &tab_name, const Rid &tuple_rid, Context *context)
{
	auto tab = db_.get_table(tab_name); // 获取表元数据
	// 索引文件删除key
	for (IndexMeta &index : tab.indexes)
	{
		std::vector<ColMeta> index_cols = index.cols;									  // 索引包含的字段
		std::string index_file_name = ix_manager_->get_index_name(tab_name, index_cols);  // 获得索引所在文件名
		char *key = new char[index.col_tot_len];										  // 索引字段下的key
		char *data = fhs_.at(tab_name).get()->get_record(tuple_rid, context).get()->data; // 这条记录的序列化数据
		int offset = 0;
		for (size_t i = 0; i < index_cols.size(); i++)
		{ // 获取record下的key
			std::memcpy(key + offset, data + index_cols[i].offset, index_cols[i].len);
			offset += index_cols[i].len;
		}
		ihs_.at(index_file_name).get()->delete_entry(key, nullptr); // 删除
	}
	// 表文件删除记录
	fhs_.at(tab_name).get()->delete_record(tuple_rid, context);
}

void SmManager::rollback_update(const std::string &tab_name,
								const Rid &tuple_rid,
								const RmRecord &record,
								Context *context)
{
	auto tab = db_.get_table(tab_name); // 获取表元数据
	// 索引文件更新key
	for (IndexMeta &index : tab.indexes)
	{
		std::vector<ColMeta> index_cols = index.cols;									  // 索引包含的字段
		std::string index_file_name = ix_manager_->get_index_name(tab_name, index_cols);  // 获得索引所在文件名
		char *key = new char[index.col_tot_len];										  // 索引字段下的key
		char *data = fhs_.at(tab_name).get()->get_record(tuple_rid, context).get()->data; // 这条记录的序列化数据
		int offset = 0;
		for (size_t i = 0; i < index_cols.size(); i++)
		{ // 获取record下的key
			std::memcpy(key + offset, data + index_cols[i].offset, index_cols[i].len);
			offset += index_cols[i].len;
		}
		ihs_.at(index_file_name).get()->delete_entry(key, nullptr);
	}
	fhs_.at(tab_name).get()->update_record(tuple_rid, record.data, context);
	for (IndexMeta &index : tab.indexes)
	{
		std::vector<ColMeta> index_cols = index.cols;									 // 索引包含的字段
		std::string index_file_name = ix_manager_->get_index_name(tab_name, index_cols); // 获得索引所在文件名
		char *key = new char[index.col_tot_len];
		char *data = record.data; // 这条记录的序列化数据
		int offset = 0;
		for (size_t i = 0; i < index_cols.size(); i++)
		{ // 获取record下的key
			std::memcpy(key + offset, data + index_cols[i].offset, index_cols[i].len);
			offset += index_cols[i].len;
		}
		ihs_.at(index_file_name).get()->insert_entry(key, tuple_rid, context->txn_);
	}
}
void SmManager::load_data_into_table(std::string &tab_name, std::string &file_name)
{
	std::ifstream csv_data(file_name);
	std::string line;
	if (!csv_data.is_open())
		throw InternalError("file cant open");
	std::istringstream sin;
	std::vector<Value> vals;
	std::string word;
	std::getline(csv_data, line);
	sin.str(line);
	bool first = true;
	int curr_offset = 0;
	TabMeta tab;
	tab.name = tab_name;
	while (std::getline(sin, word, ','))
	{
		ColMeta col = {
			.tab_name = tab_name,
			.name = word,
			.type = TYPE_INVALID,
			.len = 0,
			.offset = 0,
			.index = false};
		tab.cols.push_back(col);
	}
	while (std::getline(csv_data, line))
	{
		sin.clear();
		sin.str(line);
		vals.clear();
		for (int i = 0; i < tab.cols.size(); ++i)
		{
			std::getline(sin, word, ',');
			if (word.back() == '\r')
				word.erase(word.size() - 1, 1);
			if (first)
			{
				auto v = LoadData::trans(word);
				vals.push_back(v);
				if (v.type == TYPE_INT || v.type == TYPE_FLOAT)
					tab.cols.at(i).len = 4;
				else if (v.type == TYPE_DATETIME || v.type == TYPE_BIGINT)
					tab.cols.at(i).len = 8;
				else if (v.type == TYPE_STRING)
					tab.cols.at(i).len = v.str_val.size();
				tab.cols.at(i).type = v.type;
				tab.cols.at(i).offset = curr_offset;
				tab.cols.at(i).type = v.type;
				curr_offset += tab.cols.at(i).len;
			}
			else
			{
				auto v = LoadData::trans(word, tab.cols.at(i));
				vals.push_back(v);
			}
		}
		if (first)
		{
			first = false;
			rm_manager_->create_file(tab_name, curr_offset);
			db_.tabs_[tab_name] = tab;
			fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));
			flush_meta();
		}
		auto &fh_ = fhs_.at(tab_name);
		RmRecord rec(curr_offset);
		for (size_t i = 0; i < vals.size(); i++)
		{
			auto &col = tab.cols[i];
			auto &val = vals[i];
			val.init_raw(col.len);
			memcpy(rec.data + col.offset, val.raw->data, col.len);
		}
		fh_->insert_record(rec.data, nullptr);
	}
	// buffer_pool_manager_->flush_all_pages();
}
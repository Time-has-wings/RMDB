/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
		http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_block_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

const char *help_info = "Supported SQL syntax:\n"
						"  command ;\n"
						"command:\n"
						"  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
						"  DROP TABLE table_name\n"
						"  CREATE INDEX table_name (column_name)\n"
						"  DROP INDEX table_name (column_name)\n"
						"  INSERT INTO table_name VALUES (value [, value ...])\n"
						"  DELETE FROM table_name [WHERE where_clause]\n"
						"  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
						"  SELECT selector FROM table_name [WHERE where_clause]\n"
						"type:\n"
						"  {INT | FLOAT | CHAR(n)}\n"
						"where_clause:\n"
						"  condition [AND condition ...]\n"
						"condition:\n"
						"  column op {column | value}\n"
						"column:\n"
						"  [table_name.]column_name\n"
						"op:\n"
						"  {= | <> | < | > | <= | >=}\n"
						"selector:\n"
						"  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context)
{
	if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan))
	{
		switch (x->tag)
		{
		case T_CreateTable:
		{
			sm_manager_->create_table(x->tab_name_, x->cols_, context);
			break;
		}
		case T_DropTable:
		{
			sm_manager_->drop_table(x->tab_name_, context);
			break;
		}
		case T_CreateIndex:
		{
			sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
			break;
		}
		case T_DropIndex:
		{
			sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
			break;
		}
		default:
			throw InternalError("Unexpected field type");
			break;
		}
	}
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context)
{
	if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan))
	{
		switch (x->tag)
		{
		case T_Help:
		{
			memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
			*(context->offset_) = strlen(help_info);
			break;
		}
		case T_ShowTable:
		{
			sm_manager_->show_tables(context);
			break;
		}
		case T_DescTable:
		{
			sm_manager_->desc_table(x->tab_name_, context);
			break;
		}
		case T_Transaction_begin:
		{
			// 显示开启一个事务
			context->txn_->set_txn_mode(true);
			break;
		}
		case T_Transaction_commit:
		{
			context->txn_ = txn_mgr_->get_transaction(*txn_id);
			txn_mgr_->commit(context->txn_, context->log_mgr_);
			break;
		}
		case T_Transaction_rollback:
		{
			context->txn_ = txn_mgr_->get_transaction(*txn_id);
			txn_mgr_->abort(context->txn_, context->log_mgr_);
			break;
		}
		case T_Transaction_abort:
		{
			context->txn_ = txn_mgr_->get_transaction(*txn_id);
			txn_mgr_->abort(context->txn_, context->log_mgr_);
			break;
		}
		case T_Showindex:
		{
			sm_manager_->show_indexes(x->tab_name_, context);
			break;
		}
		case T_LoadData:
		{
			sm_manager_->load_data_into_table(x->tab_name_, x->file_name_);
			break;
		}

		default:
			throw InternalError("Unexpected field type");
			break;
		}
	}
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
							Context *context)
{
	std::vector<std::string> captions;
	captions.reserve(sel_cols.size());
	for (auto &sel_col : sel_cols)
	{
		if (sel_col.isGroup)
		{
			captions.emplace_back(sel_col.as_name);
			break;
		}
		else
			captions.emplace_back(sel_col.col_name);
	}
	// Print header into buffer

	std::fstream outfile;
	RecordPrinter rec_printer(sel_cols.size());
	if (outputfile)
	{
		rec_printer.print_separator(context);
		rec_printer.print_record(captions, context);
		rec_printer.print_separator(context);
		// print header into file
		outfile.open("output.txt", std::ios::out | std::ios::app);
		outfile << "|";
		for (int i = 0; i < captions.size(); ++i)
		{
			outfile << " " << captions[i] << " |";
		}
		outfile << "\n";
	}

	// Print records
	size_t num_rec = 0;
	std::vector<std::string> columns;
	columns.reserve(sel_cols.size());
	// 执行query_plan
	for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple())
	{
		auto Tuple = executorTreeRoot->Next();
		columns.clear();
		for (auto &col : executorTreeRoot->cols())
		{
			std::string col_str;
			char *rec_buf = Tuple->data + col.offset;
			if (col.type == TYPE_INT)
				col_str = std::to_string(*(int *)rec_buf);

			else if (col.type == TYPE_FLOAT)
			{
				col_str = std::to_string(*(float *)rec_buf);
			}
			else if (col.type == TYPE_BIGINT)
			{
				col_str = std::to_string(*(int64_t *)rec_buf);
			}
			else if (col.type == TYPE_STRING)
			{
				col_str = std::string((char *)rec_buf, col.len);
				col_str.resize(strlen(col_str.c_str()));
			}
			else if (col.type == TYPE_DATETIME)
			{
				int64_t temp = *(int64_t *)rec_buf;
				col_str = datetime::trans_datetime(temp);
			}
			columns.emplace_back(col_str);
		}
		if (outputfile)
		{
			rec_printer.print_record(columns, context);
			// print record into file
			outfile << "|";
			for (int i = 0; i < columns.size(); ++i)
			{
				outfile << " " << columns[i] << " |";
			}
			outfile << "\n";
			num_rec++;
		}
	}
	if (outputfile)
	{
		outfile.close();
		// Print footer into buffer
		rec_printer.print_separator(context);
		// Print record count into buffer
		RecordPrinter::print_record_count(num_rec, context);
	}
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec)
{
	exec->Next();
}
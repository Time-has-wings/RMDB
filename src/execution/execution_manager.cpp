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
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple())
    {
        auto Tuple = executorTreeRoot->Next(); // 啥都不干 只要Next()函数不寄
    }
    std::vector<std::string> captions;
    std::string func_name;
    bool has_group = false;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols)
    {
        if (sel_col.isGroup)
        {
            captions.push_back(sel_col.as_name);
            func_name = sel_col.func_name;
            has_group = true;
            break;
        }
        else
            captions.push_back(sel_col.col_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "|";
    for (int i = 0; i < captions.size(); ++i)
    {
        outfile << " " << captions[i] << " |";
    }
    outfile << "\n";

    // Print records
    size_t num_rec = 0;
    int cnt = 0;
    Value res;
    Value temp;
    bool first = true;
    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple())
    {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols())
        {
            if (has_group && func_name == "COUNT")
            {
                cnt++;
                continue;
            }
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT)
            {
                if (has_group)
                {
                    if (first)
                    {
                        res.set_int(*(int *)rec_buf);
                        first = false;
                    }
                    else
                    {
                        temp.set_int(*(int *)rec_buf);

                        if (func_name == "SUM")
                            res += temp;
                        else if (func_name == "MAX")
                            res.set_int(res > temp ? res.int_val : temp.int_val);
                        else if (func_name == "MIN")
                            res.set_int(res < temp ? res.int_val : temp.int_val);
                    }
                    continue;
                }
                col_str = std::to_string(*(int *)rec_buf);
            }
            else if (col.type == TYPE_FLOAT)
            {
                if (has_group)
                {
                    if (first)
                    {
                        res.set_float(*(float *)rec_buf);
                        first = false;
                    }
                    else
                    {
                        temp.set_float(*(float *)rec_buf);
                        if (func_name == "SUM")
                            res += temp;
                        else if (func_name == "MAX")
                            res.set_float(res > temp ? res.float_val : temp.float_val);
                        else if (func_name == "MIN")
                            res.set_float(res < temp ? res.float_val : temp.float_val);
                    }
                    continue;
                }
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
                if (has_group)
                {
                    if (first)
                    {
                        res.set_str(col_str);
                        first = false;
                    }
                    else
                    {
                        temp.set_str(col_str);
                        if (func_name == "MAX")
                            res.set_str(res > temp ? res.str_val : temp.str_val);
                        else if (func_name == "MIN")
                            res.set_str(res < temp ? res.str_val : temp.str_val);
                    }
                    continue;
                }
            }
            else if (col.type == TYPE_DATETIME)
            {
                int64_t temp = *(int64_t *)rec_buf;
                col_str = datetime::trans_datetime(temp);
            }
            columns.push_back(col_str);
        }
        if (!has_group)
        { // print record into buffer
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
    if (has_group)
    {
        std::string ans;
        if (func_name == "COUNT")
            ans = std::to_string(cnt);
        else
        {
            switch (res.type)
            {
            case TYPE_INT:
                ans = std::to_string(res.int_val);
                break;
            case TYPE_FLOAT:
                ans = std::to_string(res.float_val);
                break;
            case TYPE_STRING:
                ans = res.str_val;
            default:
                break;
            }
        }
        // print record into file
        outfile << "|";
        outfile << " " << ans << " |";
        outfile << "\n";
        rec_printer.print_record(std::vector<std::string>{ans}, context);
        num_rec++;
    }
    outfile.close();
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec)
{
    exec->Next();
}
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

#include <map>
#include <unordered_map>
#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"

class RedoLogsInPage
{
public:
    RedoLogsInPage() { table_file_ = nullptr; }
    RmFileHandle *table_file_;
    std::vector<lsn_t> redo_logs_; // 在该page上需要redo的操作的lsn
};

class RecoveryManager
{
public:
    RecoveryManager(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, SmManager *sm_manager)
    {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
    }

    void analyze();
    void redo();
    void undo();
    void insert_record(char *, char *, Rid &);
    void delete_record(char *, Rid &);
    void update_record(char *, char *, Rid &);

private:
    LogBuffer buffer_;                       // 读入日志
    DiskManager *disk_manager_;              // 用来读写文件
    BufferPoolManager *buffer_pool_manager_; // 对页面进行读写
    SmManager *sm_manager_;                  // 访问数据库元数据
    std::vector<std::pair<txn_id_t, lsn_t>> ATT;
    std::vector<page_id_t> DPT;
    lsn_t redo_lsn = INT32_MAX;
};
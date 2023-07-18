/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include "recovery/log_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager)
{
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    std::unique_lock<std::mutex> lock(latch_);
    if (txn == nullptr)
    {
        txn = new Transaction(next_txn_id_);
        next_txn_id_++; // 自增
    }
    txn_map.emplace(txn->get_transaction_id(), txn); // 加入全局事务表
    lock.unlock();
    BeginLogRecord beginLog(txn->get_transaction_id());
    beginLog.prev_lsn_ = txn->get_prev_lsn();  // 设置日志的prev_lsn
    log_manager->add_log_to_buffer(&beginLog); // 写入日志缓冲区
    txn->set_prev_lsn(beginLog.lsn_);
    log_manager->flush_log_to_disk(); // 写入日志缓冲区
    return txn;                       // 4
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction *txn, LogManager *log_manager)
{
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 提交所有写操作
    auto write_set = txn->get_write_set();
    while (write_set->size())
        write_set->pop_back();
    // 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto i = lock_set->begin(); i != lock_set->end(); i++)
    {
        lock_manager_->unlock(txn, *i);
    }
    // 释放资源
    lock_set->clear();
    // 刷入磁盘
    CommitLogRecord commitLog(txn->get_transaction_id());
    commitLog.prev_lsn_ = txn->get_prev_lsn();  // 设置日志的prev_lsn
    log_manager->add_log_to_buffer(&commitLog); // 写入日志缓冲区
    txn->set_prev_lsn(commitLog.lsn_);
    log_manager->flush_log_to_disk();
    // 更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction *txn, LogManager *log_manager)
{
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    // 回滚
    auto write_set = txn->get_write_set();
    while (write_set->size())
    {
        Context *context = new Context(lock_manager_, log_manager, txn);
        WType &cur_type = write_set->back()->GetWriteType();
        if (cur_type == WType::INSERT_TUPLE)
        {
            sm_manager_->rollback_insert(write_set->back()->GetTableName(), write_set->back()->GetRid(), context);
        }
        else if (cur_type == WType::DELETE_TUPLE)
        {
            sm_manager_->rollback_delete(write_set->back()->GetTableName(), write_set->back()->GetRid(), write_set->back()->GetRecord(), context);
        }
        else if (cur_type == WType::UPDATE_TUPLE)
        {
            sm_manager_->rollback_update(write_set->back()->GetTableName(), write_set->back()->GetRid(), write_set->back()->GetRecord(), context);
        }
        write_set->pop_back(); // 删除
    }
    // 释放锁
    auto lock_set = txn->get_lock_set();
    for (auto it = lock_set->begin(); it != lock_set->end(); it++)
        lock_manager_->unlock(txn, *it);
    // 清空
    lock_set->clear();
    // 刷入
    AbortLogRecord abortLog(txn->get_transaction_id());
    abortLog.prev_lsn_ = txn->get_prev_lsn();  // 设置日志的prev_lsn
    log_manager->add_log_to_buffer(&abortLog); // 写入日志缓冲区
    log_manager->flush_log_to_disk();
    // 更新
    txn->set_state(TransactionState::ABORTED);
}

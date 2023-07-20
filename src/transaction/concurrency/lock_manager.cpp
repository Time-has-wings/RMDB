/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd)
{
    std::unique_lock<std::mutex> lock{latch_};
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 本事务已对改数据项上锁(S or X)
        lock.unlock();
        return true;
    }
    if (lock_table_[newid].group_lock_mode_ != GroupLockMode::X)
    {
        if (lock_IS_on_table(txn, tab_fd)) // 可对table上IS锁
        {
            txn->get_lock_set()->insert(newid);
            LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
            newquest->granted_ = true;
            lock_table_[newid].request_queue_.push_back(*newquest);
            lock_table_[newid].group_lock_mode_ = GroupLockMode::S;
            lock.unlock();
            return true;
        }
        else
        {
            lock.unlock();
            return false;
        }
    }
    else
    {
        txn->set_state(TransactionState::ABORTED);
        lock.unlock();
        return false;
    }
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd)
{
    std::unique_lock<std::mutex> lock{latch_};
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    {
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
        {
            if (i->txn_id_ == txn->get_transaction_id())
            {
                if (lock_IX_on_table(txn, tab_fd)) // 可对table上IX锁
                {
                    i->lock_mode_ = LockMode::EXLUCSIVE;
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
                    lock.unlock();
                    return true;
                }
            }
            //存在其他事务对该数据项的锁,故本事务上X锁失败
            txn->set_state(TransactionState::ABORTED);
            lock.unlock();
            return false;
        }
    }
    if (lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK)
    {
        if (lock_IX_on_table(txn, tab_fd))
        {
            txn->get_lock_set()->insert(newid);
            LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
            newquest->granted_ = true;
            lock_table_[newid].request_queue_.push_back(*newquest);
            lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
            lock.unlock();
            return true;
        }
    }
    txn->set_state(TransactionState::ABORTED);
    lock.unlock();
    return false;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction *txn, int tab_fd)
{
    std::unique_lock<std::mutex> lock{latch_};
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 原先有加锁 说明如果存在其他事务,其他事务的可能级别为IS,IX,S,SIX
        bool flag = true;
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
        {
            if (i->txn_id_ != txn->get_transaction_id() && i->granted_)
            { // 判断非txn事务的锁是否与本事务加S锁冲突
                if (i->lock_mode_ == LockMode::EXLUCSIVE ||
                    i->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                    i->lock_mode_ == LockMode::S_IX)
                {
                    flag = false;
                    break;
                }
            }
        }
        if (!flag)
        {
            lock.unlock();
            return false;
        }
        else
        {
            for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            {
                if (i->txn_id_ == txn->get_transaction_id())
                {
                    if (i->lock_mode_ == LockMode::INTENTION_SHARED)
                    { // IS锁,其他事务可能是S,IS
                        i->lock_mode_ = LockMode::SHARED;
                        lock_table_[newid].group_lock_mode_ = GroupLockMode::S;
                    }
                    else if (i->lock_mode_ == LockMode::INTENTION_EXCLUSIVE)
                    { // IX锁,其他事务可能是S,IS
                        i->lock_mode_ = LockMode::S_IX;
                        lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX;
                    }
                }
            }
            lock.unlock();
            return true;
        }
    }
    if (!(lock_table_[newid].group_lock_mode_ != GroupLockMode::S &&
          lock_table_[newid].group_lock_mode_ != GroupLockMode::IS &&
          lock_table_[newid].group_lock_mode_ != GroupLockMode::NON_LOCK))
    {
        txn->get_lock_set()->insert(newid);
        LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
        newquest->granted_ = true;
        lock_table_[newid].request_queue_.push_back(*newquest);
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::IX)
            lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX;
        else if (lock_table_[newid].group_lock_mode_ == GroupLockMode::IS || lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK)
            lock_table_[newid].group_lock_mode_ = GroupLockMode::S;
        lock.unlock();
        return true;
    }
    else
    {
        txn->set_state(TransactionState::ABORTED);
        lock.unlock();
        return false;
    }
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd)
{

    std::unique_lock<std::mutex> lock{latch_}; // 1
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 原先有加锁 说明如果存在其他事务,其他事务的可能级别为IS,IX,S,SIX
        bool flag = true;
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
        {
            if (i->txn_id_ != txn->get_transaction_id() && i->granted_)
            { // 判断非txn事务的锁是否与本事务加X锁冲突(存在其他事务就冲突)
                flag = false;
                break;
            }
        }
        if (!flag)
        {
            lock.unlock();
            return false;
        }
        else
        {
            for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            {
                if (i->txn_id_ == txn->get_transaction_id())
                {
                    i->lock_mode_ = LockMode::EXLUCSIVE;
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
                }
            }
            lock.unlock();
            return true;
        }
    }
    if (lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK) // 4.2&5 group mode judgement
    {
        txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
        LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
        newquest->granted_ = true;
        lock_table_[newid].request_queue_.push_back(*newquest);
        lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
        lock.unlock();
        return true;
    }
    else
    {
        txn->set_state(TransactionState::ABORTED);
        return false;
    }
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd)
{

    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 原先有加锁 说明如果存在其他事务,其他事务的可能级别为IS,IX,S,SIX
        bool flag = true;
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
        {
            if (i->txn_id_ != txn->get_transaction_id() && i->granted_)
            { // 判断非txn事务的锁是否与本事务加IS锁冲突
                if (i->lock_mode_ == LockMode::EXLUCSIVE)
                {
                    flag = false;
                    break;
                }
            }
        }
        if (!flag)
        {
            return false;
        }
        else
        {
            return true;
        }
    }
    if (lock_table_[newid].group_lock_mode_ != GroupLockMode::X) // 4.2&5 group mode judgement
    {
        txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
        LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
        newquest->granted_ = true;
        lock_table_[newid].request_queue_.push_back(*newquest);
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK)
            lock_table_[newid].group_lock_mode_ = GroupLockMode::IS;
        return true;
    }
    else
    {
        txn->set_state(TransactionState::ABORTED);
        return false;
    }
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd)
{
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    { // 原先有加锁 说明如果存在其他事务,其他事务的可能级别为IS,IX,S,SIX
        bool flag = true;
        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
        {
            if (i->txn_id_ != txn->get_transaction_id() && i->granted_)
            { // 判断非txn事务的锁是否与本事务加IS锁冲突
                if (i->lock_mode_ == LockMode::SHARED ||
                    i->lock_mode_ == LockMode::EXLUCSIVE ||
                    i->lock_mode_ == LockMode::S_IX)
                {
                    flag = false;
                    break;
                }
            }
        }
        if (!flag)
        {
            return false;
        }
        else
        {
            for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++)
            {
                if (i->txn_id_ == txn->get_transaction_id())
                {
                    if (i->lock_mode_ == LockMode::SHARED)
                    { // 本事务S锁,其他事务最多是IS,IX
                        i->lock_mode_ = LockMode::S_IX;
                        lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX;
                    }
                    else if (i->lock_mode_ == LockMode::INTENTION_SHARED)
                    { // IS锁,其他事务最多是IS,IX
                        i->lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                        lock_table_[newid].group_lock_mode_ = GroupLockMode::IX;
                    }
                }
            }
            return true;
        }
    }
    if (!(lock_table_[newid].group_lock_mode_ == GroupLockMode::X &&
          lock_table_[newid].group_lock_mode_ == GroupLockMode::S &&
          lock_table_[newid].group_lock_mode_ == GroupLockMode::SIX)) // 4.2&5 group mode judgement
    {
        txn->get_lock_set()->insert(newid); // 4.1 put into lock_table
        LockRequest *newquest = new LockRequest(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
        newquest->granted_ = true;
        lock_table_[newid].request_queue_.push_back(*newquest);
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::S)
            lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX;
        else if (lock_table_[newid].group_lock_mode_ == GroupLockMode::IS || lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK)
            lock_table_[newid].group_lock_mode_ = GroupLockMode::IX;
        return true;
    }
    else
    {
        txn->set_state(TransactionState::ABORTED);
        return false;
    }
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */

txn_id_t unlock_txn_id; //全局变量 用于remove_if
bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id)
{
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::SHRINKING);
    if (txn->get_lock_set()->find(lock_data_id) != txn->get_lock_set()->end())
    {
        // 删除该事务
        unlock_txn_id = txn->get_transaction_id();
        lock_table_[lock_data_id].request_queue_.remove_if([](LockRequest it){return it.txn_id_ == unlock_txn_id;});
        // 重新评判队列的锁模式
        GroupLockMode mode = GroupLockMode::NON_LOCK;
        for (auto i = lock_table_[lock_data_id].request_queue_.begin(); i != lock_table_[lock_data_id].request_queue_.end(); i++)
        {
            if (i->granted_)
            {
                if (i->lock_mode_ == LockMode::EXLUCSIVE)
                    mode = GroupLockMode::X;
                else if (i->lock_mode_ == LockMode::SHARED && mode != GroupLockMode::SIX)
                    mode = mode == GroupLockMode::IX ? GroupLockMode::SIX : GroupLockMode::S;
                else if (i->lock_mode_ == LockMode::S_IX)
                    mode = GroupLockMode::SIX;
                else if (i->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && mode != GroupLockMode::SIX)
                    mode = mode == GroupLockMode::S ? GroupLockMode::SIX : GroupLockMode::IX;
                else if (i->lock_mode_ == LockMode::INTENTION_SHARED)
                    mode = mode == GroupLockMode::NON_LOCK ? GroupLockMode::IS : mode == GroupLockMode::IS ? GroupLockMode::IS
                                                                                                           : mode;
            }
        }
        lock_table_[lock_data_id].group_lock_mode_ = mode;
        lock.unlock();
        return true;
    }
    else
    {
        lock.unlock();
        return false;
    }
}
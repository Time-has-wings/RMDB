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

txn_id_t txn_id_for_remove; // 用于各个lock删除已经存在的锁

/**
 * @description: 采用wait-die策略
 * Wait-Die ("Old Waits for Young")
 *  → 如果请求锁事务比持有锁的事务具有更高的优先级，则请求事务等待其完成。
 *  → 否则，请求事务撤销
 * 
 * 事务对该数据项上锁,只会存在两种情况,一种是事务已经获取了对于该数据项的某类锁&这个锁在全局锁表中且属于已经获取状态,另一种就是这个锁不在全局锁表中
 * 对于第一种情况,具体情况具体分析
 * 对于第二种情况,如果相容,则直接push_back进全局锁表,并直接获得锁;否则,使用wait-die策略
 * 
 * LockRequest.granted_ 标识该锁处于wait还是已获取状态
 * 
1、修改组模式
2、修改granted以及lockmode
3、修改txn.lock_set
4、push_back入加锁队列
5、oldest更新
6、map(txn_id, lock_mode)
*/

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd)
{
    // 全局锁表互斥访问
    std::unique_lock<std::mutex> lock{latch_};
    // 事务状态判定&设置
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    // 待加锁数据项
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);

    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())   // 情形一:本事务已获得该数据项的S or X锁
    { 
        lock.unlock();
        return true;
    }
    else // 情形二: 锁表中不存在该事务对该数据项的锁
    {
        if(lock_table_[newid].group_lock_mode_ != GroupLockMode::X) // 相容(无其他事务 or 其他事务的申请全为S)时 -> 直接上锁
        {
            if(lock_IS_on_table(txn, tab_fd)) // 可对table"立刻"or"wait后"上IS锁 -> 直接获得锁
            {   
                LockRequest newreq(txn->get_transaction_id(), LockMode::SHARED);
                newreq.granted_ = true; // 锁生效
                txn->get_lock_set()->insert(newid); // 事务获得锁
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::SHARED; // map(事务id, 锁类型)
                lock_table_[newid].request_queue_.emplace_back(newreq); // 加入加锁队列
                lock_table_[newid].group_lock_mode_ = GroupLockMode::S; // 组模式(从null->S 或者 从S->S)
                lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, txn->get_transaction_id()); // 更新
                lock.unlock();
                return true;
            }
            else  // 无法对table上IS锁 -> 无法对该数据项上锁
            {
                txn->set_state(TransactionState::ABORTED); // 其实在lock_IS_on_table函数返回false时已经设置为ABORT了
                lock.unlock();
                return false;
            }
        }
        else // 不相容(存在某个事务已经获得X锁 or 有些事务还在wait X锁)时 -> 使用wait-die策略
        {
            if(txn->get_transaction_id() > lock_table_[newid].oldest_txn_id) // young wait old -> die
            {
                txn->set_state(TransactionState::ABORTED); // rollback
                lock.unlock();
                return false;
            }
            else // old wait young -> wait
            {
                // 创建申锁对象 并 push_back
                LockRequest newquest = LockRequest(txn->get_transaction_id(), LockMode::SHARED);
                lock_table_[newid].request_queue_.emplace_back(newquest);
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::SHARED; // map(事务id, 锁类型)
                // 更新oldest_txn_id(保证时间戳递减)
                lock_table_[newid].oldest_txn_id = txn->get_transaction_id(); 
                // 等待前方已经获得X锁的事务unlock 或者 等待前方在wait X锁的事务获得X锁后再unlock
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool {
                    for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++) {
                        if (i->txn_id_ != txn->get_transaction_id() && i->lock_mode_ == LockMode::EXLUCSIVE ) {
                            return false; // 等待X锁释放
                        }
                        if(i->txn_id_ == txn->get_transaction_id()) {
                            cur = &(*i);
                            return true;
                        }
                    }
                });
                // 本事务获得S锁
                cur->granted_ = true; // 修改granted属性(获得锁)
                txn->get_lock_set()->insert(newid); // txn事务真正获得该数据项的锁
                lock_table_[newid].group_lock_mode_ = GroupLockMode::S; // 修改组模式为S(wait后修改组模式)
                lock_table_[newid].cv_.notify_all(); // 唤醒本事务后一些也是上S锁的事务
                lock.unlock();
                return true;
            }
        }
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
    // 全局锁表互斥访问
    std::unique_lock<std::mutex> lock{latch_};
    // 事务状态判定&设置
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    // 待加锁数据项
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);
    
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())  // 情形一:本事务已获得该数据项的S or X锁
    {
        if(lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::EXLUCSIVE) // 本事务已经获得了该数据项的X锁
        {
            lock.unlock();
            return true; 
        }
        else // 本事务已经获得了该数据项的S锁
        {
            if(lock_table_[newid].request_queue_.size() == 1) // 加锁队列中只有本事务->直接升级X 
            { 
                if (lock_IX_on_table(txn, tab_fd)) //可对table"立刻"or"wait后"上IS锁 -> 直接升级X
                {
                    lock_table_[newid].request_queue_.begin()->lock_mode_ = LockMode::EXLUCSIVE; // 加锁队列锁类型升级
                    lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE; // map(事务id, 锁类型) 升级
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 锁模式升为X
                    lock.unlock();
                    return true;
                }
                else 
                {
                    txn->set_state(TransactionState::ABORTED); // 其实已经在lock_IX_on_table返回false之前就设置为ABORT了
                    lock.unlock();
                    return false;
                }
            }
            else // 存在其他事务对该数据项的S锁 or 存在其他事务目前在wait X锁 -> 使用wait-die策略
            {
                // 只有以下情形才有可能wait (以下假设事务id号就是时间戳)
                /*
                    事务id  5 4 3 2 1 1            事务id  5 4 3 1 2 1
                    申锁    S S S S S X            申锁    S S S S S X
                */
                if(txn->get_transaction_id() > lock_table_[newid].oldest_txn_id) 
                {
                    txn->set_state(TransactionState::ABORTED); 
                    lock.unlock();
                    return false;
                }
                else 
                {
                    if (lock_IX_on_table(txn, tab_fd)) //可对table"立刻"or"wait后"上IS锁 -> 等待前面的非txn事务unlock
                    {
                        // 将原先的S锁在加锁队列中删除 随后加入X锁
                        txn_id_for_remove = txn->get_transaction_id();
                        lock_table_[newid].request_queue_.remove_if([](LockRequest it) {return it.txn_id_ == txn_id_for_remove;});
                        // 修改组模式为X(此刻便要修改组模式)
                        lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
                        // 创建申锁对象 再加入 (这样就可以将事务为txn_id的置于所有需要考虑的锁后面, 比方情形解释的第二种情形)
                        LockRequest newreq(txn->get_transaction_id(), LockMode::EXLUCSIVE);
                        lock_table_[newid].request_queue_.emplace_back(newreq);
                        // 等待前方没有非txn事务了
                        LockRequest* cur = nullptr;
                        lock_table_[newid].cv_.wait(lock, [&]()->bool {
                        for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++) {
                                if (i->txn_id_ != txn->get_transaction_id())
                                    return false;
                                else 
                                {
                                    cur = &(*i);
                                    return true;
                                }
                            }
                        });
                        cur->granted_ = true; // 获得锁
                        lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE; // map(txn_id, LockMode)
                        lock.unlock();
                        return true;
                    }
                    else // 不能对table上IX锁
                    {
                        txn->set_state(TransactionState::ABORTED); // 其实已经在lock_IX_on_table返回false之前就设置为ABORT了
                        lock.unlock();
                        return false;
                    }
                }
            }
        }
    }
    else  // 情形二: 锁表中不存在该事务对该数据项的锁
    {
        if(lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK) // 相容(该数据项无任何事务的锁)
        {
            if(lock_IX_on_table(txn, tab_fd)) // 可对table"立刻"or"wait后"上IX锁 -> 直接获得锁
            {
                LockRequest newreq(txn->get_transaction_id(), LockMode::EXLUCSIVE);
                newreq.granted_ = true; // 锁生效
                txn->get_lock_set()->insert(newid); // 事务获得锁
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE; // map(事务id, 锁类型)
                lock_table_[newid].request_queue_.emplace_back(newreq); // 加入加锁队列
                lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, txn->get_transaction_id()); // 更新oldest_txn_id
                lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 组模式(从null->X 或者 从S->X)
                lock.unlock();
                return true;
            }
            else // 无法对table上IX锁
            {
                txn->set_state(TransactionState::ABORTED); // 其实在lock_IS_on_table函数返回false时已经设置为ABORT了
                lock.unlock();
                return false;
            }
        }
        else // 不相容(存在事务获取该数据项的锁 or wait锁)时 -> 使用wait-die策略
        {
            if(txn->get_transaction_id() > lock_table_[newid].oldest_txn_id) // young wait old -> die
            {
                txn->set_state(TransactionState::ABORTED); // rollback
                lock.unlock();
                return false;
            }
            else // old wait young -> wait
            {
                // 创建申锁对象 并 push_back
                LockRequest newquest = LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
                lock_table_[newid].request_queue_.emplace_back(newquest);
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE; // map(事务id, 锁类型)
                // 更新oldest_txn_id(保证时间戳递减)
                lock_table_[newid].oldest_txn_id = txn->get_transaction_id(); 
                // 修改组模式为X(此刻便要修改组模式,以免后续有事务申请S锁成功)
                lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
                // 等待前方没有任何事务了
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool {
                    for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++) {
                        if (i->txn_id_ != txn->get_transaction_id())
                            return false;
                        else 
                        {
                            cur = &(*i);
                            return true;
                        }
                            
                    }
                });
                // 本事务获得X锁
                cur->granted_ = true; // 修改granted属性(获得锁)
                txn->get_lock_set()->insert(newid); // txn事务真正获得该数据项的锁
                lock.unlock();
                return true;
            }
        }
    }
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
    { 
        // 因为不会对table上IS锁,故而不考虑这种情形
        if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::SHARED)
        {
            lock.unlock();
            return true;
        }
        else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::S_IX)
        {
            lock.unlock();
            return true;
        }
        else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::EXLUCSIVE)
        {
            // 本事务获得了X锁,说明不存在其他事务获得了锁 或者 有些事务的锁处于wait状态
            // 本事务获得了X锁,可以说明本事务获得了S锁
            lock.unlock();
            return true;
        }
        else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::INTENTION_EXCLUSIVE) 
        {
            // 本事务获得了IX锁, 说明其他事务获得了IX锁或IS锁(请忽略) 或者 其他事务的锁处于wait状态
            if (lock_table_[newid].request_queue_.size() == 1) // 不存在其他事务->锁升级
            {
                lock_table_[newid].request_queue_.begin()->lock_mode_ = LockMode::SHARED;
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::SHARED;
                lock_table_[newid].group_lock_mode_ = GroupLockMode::S;
                lock_table_[newid].S_exist = true;
                lock.unlock();
                return true;
            }
            else // 存在其他事务也为IX 或者 其他事务在申请S or X -> 采用wait-die策略
            {
                if (txn->get_transaction_id() > lock_table_[newid].oldest_txn_id) 
                {
                    txn->set_state(TransactionState::ABORTED);
                    lock.unlock();
                    return false;
                }
                else 
                {
                    /*
                        情形如下(假设事务id正是事务时间戳)(该情形与lock_exclusive_on_record类似)
                        事务id 5  4  3  2  1  1             事务id 5  4  3  1  2  1
                        申锁   IX IX IX IX IX S             申锁   IX IX IX IX IX S
                    */
                    // 将原先的IX锁在加锁队列中删除 随后加入S锁
                    txn_id_for_remove = txn->get_transaction_id();
                    lock_table_[newid].request_queue_.remove_if([](LockRequest it) {return it.txn_id_ == txn_id_for_remove;});
                    // 修改组模式为SIX(此刻便要修改组模式)
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX;
                    // 创建申锁对象 再加入 (这样就可以将事务为txn_id的置于所有需要考虑的锁后面, 比方情形解释的第二种情形)
                    LockRequest newreq(txn->get_transaction_id(), LockMode::S_IX);
                    lock_table_[newid].request_queue_.emplace_back(newreq);
                    // 等待前方没有非txn事务了
                    LockRequest* cur = nullptr;
                    lock_table_[newid].cv_.wait(lock, [&]()->bool {
                    for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++) {
                        if (i->txn_id_ != txn->get_transaction_id())
                            return false;
                        else 
                        {
                            cur = &(*i);
                            return true;
                            }
                        }
                    });
                    cur->granted_ = true; // 获得锁
                    lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::S_IX; // map(txn_id, LockMode)
                    lock.unlock();
                    return true;
                }
            }
        }
    }
    else // 原先该数据项不存在本事务的锁
    {
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::S || 
            lock_table_[newid].group_lock_mode_ == GroupLockMode::IS ||
            lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK) // 相容时 直接获得锁
        {
            LockRequest newreq(txn->get_transaction_id(), LockMode::SHARED);
            newreq.granted_ = true; // 锁生效
            txn->get_lock_set()->insert(newid); // 事务获得锁
            lock_table_[newid].request_queue_.emplace_back(newreq);
            lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::SHARED;
            lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, txn->get_transaction_id());
            lock_table_[newid].group_lock_mode_ = GroupLockMode::S; // 组模式一定是S, 不存在S+IX变成SIX的情况,因为该事务刚加入
            lock_table_[newid].S_exist = true; // S_exist
            lock.unlock();
            return true;
        }
        else // 不相容时,使用wait-die策略
        {
            if (txn->get_transaction_id() > lock_table_[newid].oldest_txn_id) 
            {
                txn->set_state(TransactionState::ABORTED);
                lock.unlock();
                return false;
            }
            else 
            {
                // 创建申锁对象 并 push_back
                LockRequest newquest = LockRequest(txn->get_transaction_id(), LockMode::SHARED);
                lock_table_[newid].request_queue_.emplace_back(newquest);
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::SHARED; // map(事务id, 锁类型)
                // 更新oldest_txn_id(保证时间戳递减)
                lock_table_[newid].oldest_txn_id = txn->get_transaction_id(); 
                // 组模式升级(原先因为存在IX或X或SIX而不相容)
                if (lock_table_[newid].IX_exist)
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX;
                // 等待前面所有X,IX,SIX锁unlock
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool {
                    for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++) {
                        if (i->txn_id_ != txn->get_transaction_id()) {
                            if (i->lock_mode_ == LockMode::EXLUCSIVE || 
                                i->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || 
                                i->lock_mode_ == LockMode::S_IX)
                                return false;
                        }
                        if(i->txn_id_ == txn->get_transaction_id()) {
                            cur = &(*i);
                            return true;
                        }
                    }
                });
                // 本事务获得S锁
                cur->granted_ = true; // 修改granted属性(获得锁)
                txn->get_lock_set()->insert(newid); // txn事务真正获得该数据项的锁
                lock_table_[newid].group_lock_mode_ = GroupLockMode::S; // 修改组模式为S(组模式一定为S)
                lock_table_[newid].cv_.notify_all(); // 唤醒
                lock_table_[newid].S_exist = true; // S_exist
                lock.unlock();
                return true;
            }
        }
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
    std::unique_lock<std::mutex> lock{latch_}; 
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end()) 
    { 
        // 因为不对table上IS锁,故不考虑IS
        if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::EXLUCSIVE)
        {
            lock.unlock();
            return true;
        }
        else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::S_IX)
        {
            // 本事务获得SIX锁,说明不存在其他事务获得了锁 或者 正在wait
            // 本事务获得SIX锁,应该可以说明可以上X锁
            lock.unlock();
            return true;
        }
        else if(lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::SHARED ||
                lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::INTENTION_EXCLUSIVE)
        {
            if (lock_table_[newid].request_queue_.size() == 1) // 不存在其他事务 -> 升级
            {
                if (lock_table_[newid].request_queue_.begin()->lock_mode_ == LockMode::SHARED)
                    lock_table_[newid].S_exist = false;
                else 
                    lock_table_[newid].IX_exist = false;
                lock_table_[newid].request_queue_.begin()->lock_mode_ = LockMode::EXLUCSIVE;
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE;
                lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
                lock.unlock();
                return true;
            }
            else // 使用wait-die策略
            {
                if (txn->get_transaction_id() > lock_table_[newid].oldest_txn_id)
                {
                    txn->set_state(TransactionState::ABORTED);
                    lock.unlock();
                    return false;
                }
                else 
                {
                    /*
                        情形如下(假设事务id正是事务时间戳)(该情形与lock_exclusive_on_record类似)
                        事务id 5  4  3  2  1  1             事务id 5  4  3  1  2  1
                        申锁   IX IX IX IX IX X             申锁   IX IX IX IX IX X
                        申锁   S  S  S  S  S  X             申锁   S  S  S  S  S X
                    */
                    // 将原先的IX or S锁在加锁队列中删除 随后加入X锁
                    txn_id_for_remove = txn->get_transaction_id();
                    lock_table_[newid].request_queue_.remove_if([](LockRequest it) {return it.txn_id_ == txn_id_for_remove;});
                    // 修改组模式为X(此刻便要修改组模式)
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
                    // 创建申锁对象 再加入 (这样就可以将事务为txn_id的置于所有需要考虑的锁后面, 比方情形解释的第二种情形)
                    LockRequest newreq(txn->get_transaction_id(), LockMode::EXLUCSIVE);
                    lock_table_[newid].request_queue_.emplace_back(newreq);
                    // 等待前方没有非txn事务了
                    LockRequest* cur = nullptr;
                    lock_table_[newid].cv_.wait(lock, [&]()->bool {
                    for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++) {
                        if (i->txn_id_ != txn->get_transaction_id())
                            return false;
                        else 
                        {
                            cur = &(*i);
                            return true;
                            }
                        }
                    });
                    cur->granted_ = true; // 获得锁
                    lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE; // map(txn_id, LockMode)
                    lock.unlock();
                    return true;
                }
            }
        }
        // else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::INTENTION_EXCLUSIVE)
        // {
        //     // 本事务获得了IX锁, 说明其他事务只能是IX锁或IS锁(请忽略) 也可能存在一些处于wait的锁
        //     // 可与上面合并
        // }
    }
    else //原先该数据项不存在本事务
    {
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK) // 相容时 直接获得锁
        {
            LockRequest newreq(txn->get_transaction_id(), LockMode::EXLUCSIVE);
            newreq.granted_ = true; // 锁生效
            txn->get_lock_set()->insert(newid); // 事务获得锁
            lock_table_[newid].request_queue_.emplace_back(newreq);
            lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE;
            lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, txn->get_transaction_id()); // 跟新oldest_txn_id
            lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 组模式一定是X
            lock.unlock();
            return true;
        }
        else // 不相容时 wait-die策略
        {
            if (txn->get_transaction_id() > lock_table_[newid].oldest_txn_id) {
                txn->set_state(TransactionState::ABORTED);
                lock.unlock();
                return false;
            }
            else 
            {
                // 创建申锁对象 并 push_back
                LockRequest newquest = LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
                lock_table_[newid].request_queue_.emplace_back(newquest);
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::EXLUCSIVE; // map(事务id, 锁类型)
                // 更新oldest_txn_id(保证时间戳递减)
                lock_table_[newid].oldest_txn_id = txn->get_transaction_id(); 
                // 组模式升级
                if (lock_table_[newid].group_lock_mode_ != GroupLockMode::SIX)
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::X;
                // 等待前面所有锁unlock
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool {
                    for (auto i = lock_table_[newid].request_queue_.begin(); i != lock_table_[newid].request_queue_.end(); i++) {
                        if (i->txn_id_ != txn->get_transaction_id()) 
                            return false;
                        else {
                            cur = &(*i);
                            return true;
                        }
                    }
                });
                // 本事务获得X锁
                cur->granted_ = true; // 修改granted属性(获得锁)
                txn->get_lock_set()->insert(newid); // txn事务真正获得该数据项的锁
                lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 修改组模式为X
                lock.unlock();
                return true;
            }
        }
    }
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
// IS锁没有使用过 不重要
bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd)
{
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
        return false;
    txn->set_state(TransactionState::GROWING);
    LockDataId newid(tab_fd, LockDataType::TABLE);
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end()) // 已经获得了锁(IS锁未调用过,不重要)
    {
        // 该函数未调用过(此处未实现)
    }
    else // 锁表中不存在本事务的锁
    {
        if (lock_table_[newid].group_lock_mode_ != GroupLockMode::X) // 相容 直接获得锁
        {
            LockRequest newreq(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
            newreq.granted_ = true; // 锁生效
            txn->get_lock_set()->insert(newid); // 事务获得锁
            lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::INTENTION_SHARED; // map(事务id, 锁类型)
            lock_table_[newid].request_queue_.emplace_back(newreq); // 加入加锁队列
            lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, txn->get_transaction_id()); // 更新oldest_txn_id
            if (lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK) // 更改组模式
                lock_table_[newid].group_lock_mode_ = GroupLockMode::IS;
            return true;
        }
        else // 不相容 使用wait-die策略 -> 修改为直接rollback
        {
            txn->set_state(TransactionState::ABORTED);
            return false;
        }
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
    { 
        // 因为不会对table上IS锁,故而不考虑
        if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::INTENTION_EXCLUSIVE) // 一开始获得的就是IX锁
        {
            return true;
        }
        else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::SHARED)
        {
            // 已经获得S锁,说明其他事务只能是S锁或者IS锁(请忽略)
            if (lock_table_[newid].request_queue_.size() == 1) // 只有本事务->升级
            {
                lock_table_[newid].request_queue_.begin()->lock_mode_ = LockMode::S_IX;
                lock_table_[newid].lock_txn[txn->get_transaction_id()] = LockMode::S_IX;
                lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX;
                lock_table_[newid].IX_exist = true;
                return true;
            }
            else // 本事务与其他事务一样获得了S锁,还有可能有一些锁处于wait状态 -> 修改为直接rollback
            {
                txn->set_state(TransactionState::ABORTED);
                return false;
            }
        }
        else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::EXLUCSIVE)
        {
            // ①已经获得X锁,说明其他事务没有获得锁或者还在wait ②获得了X锁,按逻辑应该也就是获得了IX锁
            lock_table_[newid].IX_exist = true;
            return true;
        }
        else if (lock_table_[newid].lock_txn[txn->get_transaction_id()] == LockMode::S_IX)
        {
            lock_table_[newid].IX_exist = true;
            return true;
        }
    }
    else // 原先该数据项不存在本事务的锁 
    {
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::X || 
            lock_table_[newid].group_lock_mode_ == GroupLockMode::S || 
            lock_table_[newid].group_lock_mode_ == GroupLockMode::SIX) // 不相容时 -> wait-die策略 -> 修改为直接rollback
        {
            txn->set_state(TransactionState::ABORTED);
            return false;
        }
        else // 相容 直接上锁
        {
            LockRequest newreq(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
            newreq.granted_ = true; 
            txn->get_lock_set()->insert(newid);
            lock_table_[newid].lock_txn[txn->get_transaction_id()] =  LockMode::INTENTION_EXCLUSIVE;
            lock_table_[newid].request_queue_.emplace_back(newreq);
            lock_table_[newid].group_lock_mode_ = GroupLockMode::IX; // 更改组模式(相容说明其他事务为IS或者IX)
            lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, txn->get_transaction_id());
            lock_table_[newid].IX_exist = true; // IX_exist 
            return true;
        }
    }
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */

txn_id_t txn_id_for_unlock; //全局变量 用于remove_if
bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id)
{
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::SHRINKING);
    if (txn->get_lock_set()->find(lock_data_id) != txn->get_lock_set()->end())
    {
        // 删除该事务
        txn_id_for_unlock = txn->get_transaction_id();
        lock_table_[lock_data_id].request_queue_.remove_if([](LockRequest it){return it.txn_id_ == txn_id_for_unlock;});
        lock_table_[lock_data_id].lock_txn.erase(txn_id_for_unlock);
        // 重新评判队列的锁模式 & IX_exist & S_exist & oldest_txn_id
        lock_table_[lock_data_id].oldest_txn_id = 1e9;
        GroupLockMode mode = GroupLockMode::NON_LOCK;
		for (auto& i : lock_table_[lock_data_id].request_queue_)
        {
            lock_table_[lock_data_id].oldest_txn_id = std::min(lock_table_[lock_data_id].oldest_txn_id, i.txn_id_); // 更新oldest
            if (i.lock_mode_ == LockMode::EXLUCSIVE)
                mode = GroupLockMode::X;
			else if (i.lock_mode_ == LockMode::SHARED)
            {
                lock_table_[lock_data_id].S_exist = true;
                if (mode != GroupLockMode::SIX) 
                    mode = mode == GroupLockMode::IX ? GroupLockMode::SIX : GroupLockMode::S;
            }
			else if (i.lock_mode_ == LockMode::S_IX)
                mode = GroupLockMode::SIX;
			else if (i.lock_mode_ == LockMode::INTENTION_EXCLUSIVE)
            {
                lock_table_[lock_data_id].IX_exist = true;
                if (mode != GroupLockMode::SIX)
                    mode = mode == GroupLockMode::S ? GroupLockMode::SIX : GroupLockMode::IX;
            }
			else if (i.lock_mode_ == LockMode::INTENTION_SHARED)
                mode = mode == GroupLockMode::NON_LOCK ? GroupLockMode::IS : mode == GroupLockMode::IS ? GroupLockMode::IS : mode;
        }
        lock_table_[lock_data_id].group_lock_mode_ = mode;
        // 唤醒事务
        lock_table_[lock_data_id].cv_.notify_all(); 
        lock.unlock();
        return true;
    }
    else
    {
        lock.unlock();
        return false;
    }
}
#include "lock_manager.h"

// record X 
bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd)
{
    // 全局锁表互斥访问
    std::unique_lock<std::mutex> lock{latch_};
    // 事务状态判定 & 设置
    if (txn->get_state() == TransactionState::ABORTED || 
        txn->get_state() == TransactionState::COMMITTED || 
        txn->get_state() == TransactionState::SHRINKING)
    {
        lock.unlock();
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 待加锁数据项
    LockDataId newid(tab_fd, rid, LockDataType::RECORD);
    // 本事务id
    txn_id_t now_txn_id = txn->get_transaction_id();
    // 本事务已经获得了该数据项的X锁
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    {
        //std::cout << now_txn_id << " " << "record_x" << " " << "1\n"; // lgm_8_16
        lock.unlock();
        return true;
    }
    else // 锁表中不存在对该数据项的锁
    {
        if (lock_table_[newid].request_queue_.empty()) // 只有加锁队列为空时,才能直接获得X锁
        {
           // std::cout << now_txn_id << " " << "record_x" << " " << "2.1\n"; // lgm_8_16
            LockRequest newreq(now_txn_id, LockMode::EXLUCSIVE);
            newreq.granted_ = true; // 1.锁生效
            txn->get_lock_set()->insert(newid); // 2.事务获得锁
            lock_table_[newid].request_queue_.push_back(newreq); // 3.加入申锁队列
            // lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 4.修改组模式(此处其实不需要)
            lock_table_[newid].oldest_txn_id = now_txn_id; // 5.更新oldest(note:获得的锁的txn_id也需要参与oldest)
            // lock_table_[newid].lock_txn[now_txn_id] = LockMode::EXLUCSIVE; // 6.维护map(txn_id, LockMode)(此处其实不需要)
			lock.unlock();
            return true;
        }
        else // 使用wait-die策略
        {
            if (now_txn_id > lock_table_[newid].oldest_txn_id)
            {
               // std::cout << now_txn_id << " " << "record_x" << " " << "2.2.1\n"; // lgm_8_16
                txn->set_state(TransactionState::ABORTED);
                lock.unlock();
                return false;
            }
            else
            {
               // std::cout << now_txn_id << " " << "record_x" << " " << "2.2.2\n"; // lgm_8_16
                // 创建加锁对象 & 加入加锁队列 
                LockRequest newreq(now_txn_id, LockMode::EXLUCSIVE);
                lock_table_[newid].request_queue_.push_back(newreq);
                // 更新oldest
                lock_table_[newid].oldest_txn_id = now_txn_id;
                // 等待前方所有X锁
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool
                {
                    for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                    {
                        if (it->txn_id_ != now_txn_id)
                            return false;
                        else
                        {
                            cur = &(*it);
                            return true;
                        }
                    }
                });
                // 修改操作
                cur->granted_ = true; // 锁生效
                txn->get_lock_set()->insert(newid); // 本事务真正获得锁
                // 无须考虑组模式
                lock_table_[newid].cv_.notify_all(); // lgm_test
                lock.unlock();
                return true;
            }
        }
    }
}


/*
    分析
        一、
            相容矩阵如下
                IX  S   X
            IX  Y   N   N
            S   N   Y   N
            X   N   N   N
            同时需要注意,尽管相容,还要去考虑获得锁之后,会不会出现正在wait的事务id会大于刚因为相容而赋予的事务id
            (比方以下情形
            
            事务2获得了表1的锁
            然后事务2申请表2的时候 因为事务3 然后wait
            之后事务1去申请表2 因为相容，如果这个时候直接上锁，就间接事务2在等事务1
            然后事务1还是活的
            它去申请表1，而因为不相容，要等事务2
        二、
            直接获得锁时push_front
            需要wait时push_back
            由此保证加锁队列中上面的锁为got,后面的锁为waiting
    

*/

txn_id_t txn_id_for_remove; // 用于临时remove
// table IX
bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd)
{
    // 全局锁表互斥访问
    std::unique_lock<std::mutex> lock{latch_};
    // 事务状态判定 & 设置
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
    {
        lock.unlock();
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 待加锁数据项
    LockDataId newid(tab_fd, LockDataType::TABLE);
    // 本事务id
    txn_id_t now_txn_id = txn->get_transaction_id();
    // 本事务已经获得了该数据项的锁
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    {
        LockMode mode = lock_table_[newid].lock_txn[now_txn_id];
        if (mode == LockMode::INTENTION_EXCLUSIVE) // 之前便获得了IX锁
        {
            //std::cout << now_txn_id << " " << "table_IX" << " " << "1.1\n"; // lgm_8_16
            lock.unlock();
            return true;
        }
        else if (mode == LockMode::S_IX) // 之前就获得了SIX锁
        {
            //std::cout << now_txn_id << " " << "table_IX" << " " << "1.2\n"; // lgm_8_16
            lock.unlock();
            return true;
        }
        else if (mode == LockMode::SHARED)
        {
            /*
                分析:本事务获得了S锁
                    1.加锁队列只有本事务->直接升级锁
                    2.加锁队列中存在其他事务
                        2.1 有事务got了,则这个锁一定是S锁
                        2.2 若存在wait队列,则根据wait-die策略,本事务将rollback
                        2.3 若没有wait队列,则将本事务的锁先remove,再push_back本锁,同时设置LockRequest.waiting_lock_mode为IX锁,以便使其成为wait队列的第一个
                        2.4 以上判断有无加锁队列,可通过比较oldest判断
            */
            if (lock_table_[newid].request_queue_.size() == 1) // 只有本事务->锁直接升级
            {
               // std::cout << now_txn_id << " " << "table_IX" << " " << "1.3.1\n"; // lgm_8_16
                lock_table_[newid].request_queue_.begin()->lock_mode_ = LockMode::S_IX; // 加锁对象更新
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::S_IX; // 维护map(txn_id, LockMode)
                lock_table_[newid].group_lock_mode_= GroupLockMode::SIX; // 更新组模式
                lock.unlock();
                return true;
            }
            else // 存在其他事务
            {
                if (now_txn_id > lock_table_[newid].oldest_txn_id)
                {
                   // std::cout << now_txn_id << " " << "table_IX" << " " << "1.3.2.1\n"; // lgm_8_16
                    txn->set_state(TransactionState::ABORTED);
                    lock.unlock();
                    return false;
                }
                else // 没有正在waiting的事务,说明所有其他事务都是S锁,且均获得
                {
                   // std::cout << now_txn_id << " " << "table_IX" << " " << "1.3.2.2\n"; // lgm_8_16
                    // 首先删除原来的锁
                    txn_id_for_remove = now_txn_id;
                    lock_table_[newid].request_queue_.remove_if([](LockRequest it){return it.txn_id_ == txn_id_for_remove;});
                    // 生成加锁对象 & push_back
                    LockRequest newreq(now_txn_id, mode); // got的还是原来的S锁(即mode变量)
                    newreq.granted_ = true; // 本事务还是获得了S锁
                    newreq.waiting_lock_mode_ = LockMode::INTENTION_EXCLUSIVE; // 本事务正在wait IX锁
                    lock_table_[newid].request_queue_.push_back(newreq);
                    // 等待前面的非now_txn_id事务释放锁
                    LockRequest* cur = nullptr;
                    lock_table_[newid].cv_.wait(lock, [&]()->bool
                    {
                        for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                        {
                            if (it->txn_id_ != now_txn_id)
                                return false;
                            else 
                            {
                                cur = &(*it);
                                return true;
                            }
                        }
                    });
                    // wait结束
                    cur->lock_mode_ = LockMode::S_IX;
                    cur->waiting_lock_mode_ = LockMode::NO_LOCK; // 没有在等待了
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX; // 更新组模式
                    lock_table_[newid].lock_txn[now_txn_id] = LockMode::S_IX; // 维护map(txn_id, LockMode)
                    lock_table_[newid].cv_.notify_all(); // lgm_test
                    lock.unlock();
                    return true;
                }
            }
        }
        else if (mode == LockMode::EXLUCSIVE)
        {
          //  std::cout << now_txn_id << " " << "table_IX" << " " << "1.4\n"; // lgm_8_16
            // 都已经获得了X锁了,IX锁就相当于获得了
            lock.unlock();
            return true;
        }
        else; // 不存在此情形
    }
    else 
    {
        // 相容时
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::IX ||  // 组模式为IX,即got的事务上的锁全是IX
            lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK) // 组模式为空 
        {
            // 注意此处仍然需要去判断一下waiting队列的最大id(why do it? -> 见表锁前的分析)
            txn_id_t waiting_max_id = -1;
            for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
            {
                if (it->granted_ && it->waiting_lock_mode_ != LockMode::NO_LOCK)
                {
                    waiting_max_id = it->txn_id_;
                    break;
                }
                else if (it->granted_ == false)
                {
                    waiting_max_id = it->txn_id_;
                    break;
                }
                else;
            }
            // 比较一下waiting_max_id
            if (now_txn_id > waiting_max_id) // 保证小的等大的 -> 直接获得锁
            {
                //std::cout << now_txn_id << " " << "table_IX" << " " << "2.1.1\n"; // lgm_8_16
                LockRequest newreq(now_txn_id, LockMode::INTENTION_EXCLUSIVE);
                newreq.granted_ = true; // 锁生效
                txn->get_lock_set()->insert(newid); // 事务获得锁
                lock_table_[newid].request_queue_.push_front(newreq); // 注意是push_front
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::INTENTION_EXCLUSIVE; // 维护map(txn_id, LockMode)
                lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, now_txn_id); // 维护oldest
                lock_table_[newid].group_lock_mode_ = GroupLockMode::IX; // 组模式(一定是IX)
                lock.unlock();
                return true;
            }
            else // wait-die策略
            {
                if (now_txn_id > lock_table_[newid].oldest_txn_id)
                {
                  //  std::cout << now_txn_id << " " << "table_IX" << " " << "2.1.2.1\n"; // lgm_8_16
                    txn->set_state(TransactionState::ABORTED);
                    lock.unlock();
                    return false;
                }
                else // wait
                {
                  //  std::cout << now_txn_id << " " << "table_IX" << " " << "2.1.2.2\n"; // lgm_8_16
                    // 创建加锁对象 & push_back
                    LockRequest newreq(now_txn_id, LockMode::INTENTION_EXCLUSIVE);
                    lock_table_[newid].request_queue_.push_back(newreq);
                    // 更新oldest
                    lock_table_[newid].oldest_txn_id = now_txn_id;
                    // 等待前面的锁都释放(这里有一点没有处理好)
                    /*
                        情形如下
                        事务id 10  9  8  7  6  5  4  3  2  1
                        申锁   IX IX IX IX IX IX  S IX IX IX
                        
                        如果按照上面所说,等待前面所有的锁释放,事务2,1就非得等待事务3结束才得到锁;其实当事务3获得锁时,事务2,1就应该可以获得锁
                    */
                    /*
                        上述修改为 当从头到自己这全是IX锁时就可以释放了
                    */
                    LockRequest* cur = nullptr;
                    lock_table_[newid].cv_.wait(lock, [&]()->bool
                    {
                        bool flag = true;
                        for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                        {
                            if (it->txn_id_ != now_txn_id)
                            {
                                if (it->lock_mode_ != LockMode::INTENTION_EXCLUSIVE) // 从头到尾存在一个不是IX锁的
                                {
                                    flag = false;
                                    break;
                                }
                            }
                            else 
                            {
                                cur = &(*it);
                                break;
                            }
                        }
                        return flag;
                    });
                    // wait结束
                    cur->granted_ = true; // 锁生效
                    lock_table_[newid].lock_txn[now_txn_id] = LockMode::INTENTION_EXCLUSIVE; // 维护map(txn_id, LockMode)
                    txn->get_lock_set()->insert(newid); // txn事务真正获得锁
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::IX; // 组模式为IX
                    lock_table_[newid].cv_.notify_all(); // 感觉这里的notify_all没用,因为上面的那个处理方案
                    lock.unlock();
                    return true;
                }
            }
        }
        else // 不相容时 -> wait-die
        {
            if (now_txn_id > lock_table_[newid].oldest_txn_id)
            {
               // std::cout << now_txn_id << " " << "table_IX" << " " << "2.2.1\n"; // lgm_8_16
                txn->set_state(TransactionState::ABORTED);
                lock.unlock();
                return false;
            }
            else 
            {
               // std::cout << now_txn_id << " " << "table_IX" << " " << "2.2.2\n"; // lgm_8_16
                // 创建加锁对象 & push_back
                LockRequest newreq(now_txn_id, LockMode::INTENTION_EXCLUSIVE);
                lock_table_[newid].request_queue_.push_back(newreq);
                // 更新oldest
                lock_table_[newid].oldest_txn_id = now_txn_id;
                // 等待前面的锁都释放(这里有一点没有处理好)(同上)
                // 后续处理策略同上
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool
                {
                    for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                    {
                        bool flag = true;
                        for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                        {
                            if (it->txn_id_ != now_txn_id)
                            {
                                if (it->lock_mode_ != LockMode::INTENTION_EXCLUSIVE) // 从头到尾存在一个不是IX锁的
                                {
                                    flag = false;
                                    break;
                                }
                            }
                            else 
                            {
                                cur = &(*it);
                                break;
                            }
                        }
                        return flag;
                    }
                });
                // wait结束
                cur->granted_ = true; // 锁生效
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::INTENTION_EXCLUSIVE; // 维护map(txn_id, LockMode)
                txn->get_lock_set()->insert(newid); // txn事务真正获得锁
                lock_table_[newid].group_lock_mode_ = GroupLockMode::IX; // 组模式为IX
                lock_table_[newid].cv_.notify_all(); // 感觉这里的notify_all没用,因为上面的那个处理方案
                lock.unlock();
                return true;
            }
        }
    }
}

// table S
bool LockManager::lock_shared_on_table(Transaction *txn, int tab_fd)
{
    // 全局锁表互斥访问
    std::unique_lock<std::mutex> lock{latch_};
    // 事务状态判定 & 设置
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
    {
        lock.unlock();
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 待加锁数据项
    LockDataId newid(tab_fd, LockDataType::TABLE);
    // 本事务id
    txn_id_t now_txn_id = txn->get_transaction_id();
    // 本事务已经获得了该数据项的锁
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    {
        LockMode mode = lock_table_[newid].lock_txn[now_txn_id];
        if (mode == LockMode::SHARED) // 之前便获得了S锁
        {
            //std::cout << now_txn_id << " " << "table_S" << " " << "1.1\n"; // lgm_8_16
            lock.unlock();
            return true;
        }
        else if (mode == LockMode::S_IX) // 之前就获得了SIX锁
        {
           // std::cout << now_txn_id << " " << "table_S" << " " << "1.2\n"; // lgm_8_16
            lock.unlock();
            return true;
        }
        else if (mode == LockMode::INTENTION_EXCLUSIVE)
        {
            /*
                分析:本事务获得了IX锁
                    1.加锁队列只有本事务->直接升级锁
                    2.加锁队列中存在其他事务
                        2.1 有事务got了,则这个锁一定是IX锁
                        2.2 若存在wait队列,则根据wait-die策略,本事务将rollback
                        2.3 若没有wait队列,则将本事务的锁先remove,再push_back本锁,同时设置LockRequest.waiting_lock_mode为S锁,以便使其成为wait队列的第一个
                        2.4 以上判断有无加锁队列,可通过比较oldest判断
            */
            if (lock_table_[newid].request_queue_.size() == 1) // 只有本事务->锁直接升级
            {
               // std::cout << now_txn_id << " " << "table_S" << " " << "1.3.1\n"; // lgm_8_16
                lock_table_[newid].request_queue_.begin()->lock_mode_ = LockMode::S_IX; // 加锁申请更新
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::S_IX; // 维护map(txn_id, LockMode)
                lock_table_[newid].group_lock_mode_= GroupLockMode::SIX; // 组模式升级
                lock.unlock();
                return true;
            }
            else // 存在其他事务
            {
                if (now_txn_id > lock_table_[newid].oldest_txn_id)
                {
                 //   std::cout << now_txn_id << " " << "table_S" << " " << "1.3.2.1\n"; // lgm_8_16
                    txn->set_state(TransactionState::ABORTED);
                    lock.unlock();
                    return false;
                }
                else // 没有正在waiting的事务,说明所有其他事务都是IX锁,且均获得
                {
                 //   std::cout << now_txn_id << " " << "table_S" << " " << "1.3.2.2\n"; // lgm_8_16
                    // 首先删除原来的锁
                    txn_id_for_remove = now_txn_id;
                    lock_table_[newid].request_queue_.remove_if([](LockRequest it){return it.txn_id_ == txn_id_for_remove;});
                    // 生成加锁对象 & push_back
                    LockRequest newreq(now_txn_id, mode); // got的还是原来的IX锁(即mode变量)
                    newreq.granted_ = true; // 本事务还是获得了IX锁
                    newreq.waiting_lock_mode_ = LockMode::SHARED; // 本事务正在wait S锁
                    lock_table_[newid].request_queue_.push_back(newreq);
                    // 等待前面的非now_txn_id事务释放锁
                    LockRequest* cur = nullptr;
                    lock_table_[newid].cv_.wait(lock, [&]()->bool
                    {
                        for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                        {
                            if (it->txn_id_ != now_txn_id)
                                return false;
                            else 
                            {
                                cur = &(*it);
                                return true;
                            }
                        }
                    });
                    // wait结束
                    cur->lock_mode_ = LockMode::S_IX;
                    cur->waiting_lock_mode_ = LockMode::NO_LOCK; // 没有在等待了
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::SIX; // 组模式更新
                    lock_table_[newid].lock_txn[now_txn_id] = LockMode::S_IX; // 维护map(txn_id, LockMode)
                    lock_table_[newid].cv_.notify_all(); // lgm_test
                    lock.unlock();
                    return true;
                }
            }
        }
        else if (mode == LockMode::EXLUCSIVE)
        {
            //std::cout << now_txn_id << " " << "table_S" << " " << "1.4\n"; // lgm_8_16
            // 都已经获得了X锁了,S锁就相当于获得了
            lock.unlock();
            return true;
        }
        else; // 不存在此情形
    }
    else 
    {
        // 相容时
        if (lock_table_[newid].group_lock_mode_ == GroupLockMode::S ||  // 组模式为S,即got的事务上的锁全是S 
            lock_table_[newid].group_lock_mode_ == GroupLockMode::NON_LOCK) // 组模式为空 
        {
            // 注意此处仍然需要去判断一下waiting队列的最大id(why do it? -> 见表锁前的分析)
            txn_id_t waiting_max_id = -1;
            for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
            {
                if (it->granted_ && it->waiting_lock_mode_ != LockMode::NO_LOCK)
                {
                    waiting_max_id = it->txn_id_;
                    break;
                }
                else if (it->granted_ == false)
                {
                    waiting_max_id = it->txn_id_;
                    break;
                }
                else;
            }
            // 比较一下waiting_max_id
            if (now_txn_id > waiting_max_id) // 保证小的等大的 -> 直接获得锁
            {
            //    std::cout << now_txn_id << " " << "table_S" << " " << "2.1.1\n"; // lgm_8_16
                LockRequest newreq(now_txn_id, LockMode::SHARED);
                newreq.granted_ = true; // 锁生效
                txn->get_lock_set()->insert(newid); // 事务获得锁
                lock_table_[newid].request_queue_.push_front(newreq); // 注意是push_front
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::SHARED; // 维护map(txn_id, LockMode)
                lock_table_[newid].oldest_txn_id = std::min(lock_table_[newid].oldest_txn_id, now_txn_id); // 维护oldest
                lock_table_[newid].group_lock_mode_ = GroupLockMode::S; // 组模式(一定是S)
                lock.unlock();
                return true;
            }
            else // wait-die策略
            {
                if (now_txn_id > lock_table_[newid].oldest_txn_id)
                {
               //     std::cout << now_txn_id << " " << "table_S" << " " << "2.1.2.1\n"; // lgm_8_16
                    txn->set_state(TransactionState::ABORTED);
                    lock.unlock();
                    return false;
                }
                else // wait
                {
               //     std::cout << now_txn_id << " " << "table_S" << " " << "2.1.2.2\n"; // lgm_8_16
                    // 创建加锁对象 & push_back
                    LockRequest newreq(now_txn_id, LockMode::SHARED);
                    lock_table_[newid].request_queue_.push_back(newreq);
                    // 更新oldest
                    lock_table_[newid].oldest_txn_id = now_txn_id;
                    // 等待前面的锁都释放(这里有一点没有处理好)
                    /*
                        情形如下
                        事务id 10 9 8 7 6 5  4 3 2 1
                        申锁    S S S S S S IX S S S
                        
                        如果按照上面所说,等待前面所有的锁释放,事务2,1就非得等待事务3结束才得到锁;其实当事务3获得锁时,事务2,1就应该可以获得锁
                    */
                    /*
                        后续处理策略采取 同IX锁
                    */
                    LockRequest* cur = nullptr;
                    lock_table_[newid].cv_.wait(lock, [&]()->bool
                    {
                        bool flag = true;
                        for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                        {
                            if (it->txn_id_ != now_txn_id)
                            {
                                if (it->lock_mode_ != LockMode::SHARED) // 从头到尾存在一个不是S锁的
                                {
                                    flag = false;
                                    break;
                                }
                            }
                            else 
                            {
                                cur = &(*it);
                                break;
                            }
                        }
                        return flag;
                    });
                    // wait结束
                    cur->granted_ = true; // 锁生效
                    lock_table_[newid].lock_txn[now_txn_id] = LockMode::SHARED; // 维护map(txn_id, LockMode)
                    txn->get_lock_set()->insert(newid); // txn事务真正获得锁
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::S; // 组模式为S
                    lock_table_[newid].cv_.notify_all(); // 感觉这里的notify_all没用,因为上面的那个处理方案
                    lock.unlock();
                    return true;
                }
            }
        }
        else // 不相容时 -> wait-die
        {
            if (now_txn_id > lock_table_[newid].oldest_txn_id)
            {
             //   std::cout << now_txn_id << " " << "table_S" << " " << "2.2.1\n"; // lgm_8_16
                txn->set_state(TransactionState::ABORTED);
                lock.unlock();
                return false;
            }
            else 
            {
              //  std::cout << now_txn_id << " " << "table_S" << " " << "2.2.2\n"; // lgm_8_16
                // 创建加锁对象 & push_back
                LockRequest newreq(now_txn_id, LockMode::SHARED);
                lock_table_[newid].request_queue_.push_back(newreq);
                // 更新oldest
                lock_table_[newid].oldest_txn_id = now_txn_id;
                // 等待前面的锁都释放(这里有一点没有处理好)(同上)
                // 后续处理策略同上
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool
                {
                    bool flag = true;
                        for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                        {
                            if (it->txn_id_ != now_txn_id)
                            {
                                if (it->lock_mode_ != LockMode::SHARED) // 从头到尾存在一个不是IX锁的
                                {
                                    flag = false;
                                    break;
                                }
                            }
                            else 
                            {
                                cur = &(*it);
                                break;
                            }
                        }
                        return flag;
                });
                // wait结束
                cur->granted_ = true; // 锁生效
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::SHARED; // 维护map(txn_id, LockMode)
                txn->get_lock_set()->insert(newid); // txn事务真正获得锁
                lock_table_[newid].group_lock_mode_ = GroupLockMode::S; // 组模式为S
                lock_table_[newid].cv_.notify_all(); // 感觉这里的notify_all没用,因为上面的那个处理方案
                lock.unlock();
                return true;
            }
        }
    }
}

// table X
bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd)
{
    // 全局锁表互斥访问
    std::unique_lock<std::mutex> lock{latch_};
    // 事务状态判定 & 设置
    if (txn->get_state() == TransactionState::ABORTED ||
        txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::SHRINKING)
    {
        lock.unlock();
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 待加锁数据项
    LockDataId newid(tab_fd, LockDataType::TABLE);
    // 本事务id
    txn_id_t now_txn_id = txn->get_transaction_id();
    // 本事务已经获得了该数据项的锁
    if (txn->get_lock_set()->find(newid) != txn->get_lock_set()->end())
    {
        LockMode mode = lock_table_[newid].lock_txn[now_txn_id];
        if (mode == LockMode::EXLUCSIVE) // 之前便获得了X锁
        {
           // std::cout << now_txn_id << " " << "table_X" << " " << "1.1\n"; // lgm_8_16
            lock.unlock();
            return true;
        }
        else if (mode == LockMode::S_IX) // 获得了SIX锁,说明其他事务全在wait本事务,故而本事务可以对表上X锁
        {
           // std::cout << now_txn_id << " " << "table_X" << " " << "1.2\n"; // lgm_8_16
            lock.unlock();
            return true;
        }
        else if (mode == LockMode::INTENTION_EXCLUSIVE || mode == LockMode::SHARED)
        {
            /*
                分析:本事务获得了IX锁,
                    1.加锁队列中只有本事务->直接升级锁
                    2.加锁队列中存在其他事务
                        2.1 有事务got了锁,则这个锁一定是IX锁
                        2.2 若存在wait队列,则根据wait-die策略,本事务直接rollback
                        2.3 没有wait队列,则将本事务的锁先remove,再push_back本锁,同时设置LockRequest.waiting_lock_mode_为X锁,以便使其成为wait队列的第一个
                        2.4 以上判断有无加锁队列,可通过比较oldest判断
            */
            if (lock_table_[newid].request_queue_.size() == 1) // 只有本事务->本事务直接升级
            {
             //   std::cout << now_txn_id << " " << "table_X" << " " << "1.3.1\n"; // lgm_8_16
                lock_table_[newid].request_queue_.begin()->lock_mode_ = LockMode::EXLUCSIVE; // 升级
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::EXLUCSIVE; // 升级
                lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 锁模式升级
                lock.unlock();
                return true;
            }
            else // 存在其他事务
            {
                if (now_txn_id > lock_table_[newid].oldest_txn_id) // 存在有waiting的事务
                {
             //       std::cout << now_txn_id << " " << "table_X" << " " << "1.3.2.1\n"; // lgm_8_16
                    txn->set_state(TransactionState::ABORTED);
                    lock.unlock();
                    return false;
                }
                else // 没有正在waiting的事务,说明所有事务都是IX锁,且均获得
                {
                 //   std::cout << now_txn_id << " " << "table_X" << " " << "1.3.2.2\n"; // lgm_8_16
                    // 首先删除原来的锁
                    txn_id_for_remove = now_txn_id;
                    lock_table_[newid].request_queue_.remove_if([](LockRequest it){return it.txn_id_ == txn_id_for_remove;});
                    // 生成加锁对象 & push_back
                    LockRequest newreq(now_txn_id, mode); // got的还是原来的IX锁(即mode变量)
                    newreq.granted_ = true; // 本事务还是获得了该数据项的IX锁
                    newreq.waiting_lock_mode_ = LockMode::EXLUCSIVE; // 本事务正在wait的锁类型为X
                    lock_table_[newid].request_queue_.push_back(newreq); // 是push_back 而不是push_front
                    // 等待前面非now_txn_id的事务释放IX锁
                    LockRequest* cur = nullptr;
                    lock_table_[newid].cv_.wait(lock, [&]()->bool
                    {
                        for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                        {
                            if (it->txn_id_ != now_txn_id)
                                return false;
                            else 
                            {
                                cur = &(*it);
                                return true;
                            }
                        }
                    });
                    // wait结束
                    cur->lock_mode_ = LockMode::EXLUCSIVE;
                    cur->waiting_lock_mode_ = LockMode::NO_LOCK; // 没有在等待了
                    lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 更新组模式
                    lock_table_[newid].lock_txn[now_txn_id] = LockMode::EXLUCSIVE; // 维护map(txn_id, LockMode)
                    lock_table_[newid].cv_.notify_all(); // lgm_test
                    lock.unlock();
                    return true;
                }
            }            
        }
        // else if (mode == LockMode::SHARED)
        // {
        //     /*
        //         分析:和IX情况完全一样 故可合并
        //     */
        // }
        else; // 不存在此情形
    }
    else 
    {
        if (lock_table_[newid].request_queue_.empty()) // 只有申锁队列为空时,可以直接获得锁
        {
           // std::cout << now_txn_id << " " << "table_X" << " " << "2.1\n"; // lgm_8_16
            LockRequest newreq(now_txn_id, LockMode::EXLUCSIVE);
            newreq.granted_ = true; // 锁生效
            txn->get_lock_set()->insert(newid); // 本事务获得锁
            lock_table_[newid].request_queue_.push_front(newreq); // 加入加锁队列
            lock_table_[newid].oldest_txn_id = now_txn_id; // 更新oldest
            lock_table_[newid].lock_txn[now_txn_id] = LockMode::EXLUCSIVE; // map(txn_id, LockMode)
            lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 更新组模式
            lock.unlock();
            return true;
        }
        else // 存在其他事务的锁 -> wait-die
        {
            if (now_txn_id > lock_table_[newid].oldest_txn_id)
            {
                //std::cout << now_txn_id << " " << "table_X" << " " << "2.2.1\n"; // lgm_8_16
                txn->set_state(TransactionState::ABORTED);
                lock.unlock();
                return false;
            }
            else 
            {
             //   std::cout << now_txn_id << " " << "table_X" << " " << "2.2.2\n"; // lgm_8_16
                // 创建加锁对象 & 加入加锁队列
                LockRequest newreq(now_txn_id, LockMode::EXLUCSIVE);
                lock_table_[newid].request_queue_.push_back(newreq);
                // 更新oldest
                lock_table_[newid].oldest_txn_id = now_txn_id;
                // 等待前方没有任何事务了
                LockRequest* cur = nullptr;
                lock_table_[newid].cv_.wait(lock, [&]()->bool
                {
                    for (auto it = lock_table_[newid].request_queue_.begin(); it != lock_table_[newid].request_queue_.end(); it++)
                    {
                        if (it->txn_id_ != now_txn_id)
                            return false;
                        else 
                        {
                            cur = &(*it);
                            return true;
                        }
                    }
                });
                // wait结束
                cur->granted_ = true; // 锁生效
                lock_table_[newid].lock_txn[now_txn_id] = LockMode::EXLUCSIVE; // 维护map(txn_id, LockMode)
                txn->get_lock_set()->insert(newid); // txn事务真正获得了锁
                lock_table_[newid].group_lock_mode_ = GroupLockMode::X; // 更新组模式
                lock_table_[newid].cv_.notify_all(); // lgm_test
                lock.unlock();
                return true;
            }
        }
    }
}

// unlock
txn_id_t txn_id_for_unlock; //全局变量 用于remove_if
bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id)
{
    // 全局锁表互斥访问
    std::unique_lock<std::mutex> lock{latch_};
    // 本事务确实有该锁
    if (txn->get_lock_set()->find(lock_data_id) != txn->get_lock_set()->end())
    {
        // 设置事务状态
        txn->set_state(TransactionState::SHRINKING);
        // 加锁队列删除该锁
        txn_id_for_unlock = txn->get_transaction_id();
        lock_table_[lock_data_id].request_queue_.remove_if([](LockRequest it){return it.txn_id_ == txn_id_for_unlock;});
        // 维护map(txn_id,LockMode)
        lock_table_[lock_data_id].lock_txn.erase(txn->get_transaction_id());
        // 重新判定组模式 & oldest
        if (lock_data_id.type_ == LockDataType::RECORD) // 行级锁
        {
            // 组模式对于record而言不重要
            if (lock_table_[lock_data_id].request_queue_.empty()) // 申锁队列为空时 重新设置oldest为较大值
                lock_table_[lock_data_id].oldest_txn_id = 2e9;
        }
        else // 表级锁
        {
            if (lock_table_[lock_data_id].request_queue_.empty())
            {
                lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::NON_LOCK; // 设置组模式
                lock_table_[lock_data_id].oldest_txn_id = 2e9; // 申锁队列为空时,才需要设计oldest
            }
            else // 此种情形oldest_txn_id不需要设置
            {
                if (lock_table_[lock_data_id].request_queue_.begin()->granted_) // 申锁队列的第一个不是waiting
                {
                    // 不需要设置组模式,组模式一定是刚刚删除的模式,也就是目前begin的模式
                }
                else 
                {
                    lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::NON_LOCK; // 其实不需要设置,其组模式将由唤醒的事务替代(注意wait结束时一定要更新组模式!!!)
                }
            }
        }
        // 唤醒事务
        lock_table_[lock_data_id].cv_.notify_all();
        // return
        lock.unlock();
        return true;
    }
    else 
    {
        lock.unlock();
        return false;
    }
}

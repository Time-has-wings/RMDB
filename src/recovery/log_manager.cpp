/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
		http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include "log_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord *log_record)
{
	std::unique_lock<std::mutex> lock{latch_};		   // 互斥访问
	if (log_buffer_.is_full(log_record->log_tot_len_)) // 日志缓冲区已满
	{
		flush_log_to_disk(); // 写入磁盘么? 这里调用会不会受上面的lock的影响啊
	}
	// 修改log_record的lsn_
	log_record->lsn_ = global_lsn_;
	global_lsn_ += log_record->log_tot_len_;
	// 写入缓冲区
	char dest[log_record->log_tot_len_];
	log_record->serialize(dest);
	memcpy(log_buffer_.buffer_ + log_buffer_.offset_, dest, log_record->log_tot_len_);
	log_buffer_.offset_ += log_record->log_tot_len_;
	lock.unlock();			 // 解锁
	return log_record->lsn_; // 返回日志记录号
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk()
{
	std::unique_lock<std::mutex> lock{latch_}; // 互斥访问
	char *log_buffer = new char[log_buffer_.offset_];
	memcpy(log_buffer, log_buffer_.buffer_, log_buffer_.offset_);
	buffers.push_front(std::pair<char *, size_t>(log_buffer, log_buffer_.offset_));
	log_buffer_.offset_ = 0;
	cond.notify_one();
}
void LogManager::run()
{
	while (true)
	{
		std::unique_lock<std::mutex> lck(mtx);
		while (buffers.empty() && !stop)
			cond.wait(lck);
		if (stop && buffers.empty())
			return;
		auto &log = buffers.back();
		buffers.pop_back();
		disk_manager_->write_log(log.first, log.second);
		delete[] log.first;
	}
}
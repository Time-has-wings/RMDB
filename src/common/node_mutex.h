#pragma once

#include <climits>  // 包含各个数据类型的最大值与最小值
#include <condition_variable>
#include <mutex>

class node_mutex
{
	static const uint32_t MaxReaders = UINT_MAX;

 public:
	node_mutex() : ReaderCount(0), WriterEntered(false)
	{
	}

	~node_mutex() = default;

	void write_lock()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		while (WriterEntered)
			Reader.wait(lock);
		WriterEntered = true;
		while (ReaderCount > 0)
			writer_.wait(lock);
	}

	void write_unlock()
	{
		std::lock_guard<std::mutex> guard(mutex_);
		WriterEntered = false;
		Reader.notify_all();
	}

	void read_lock()
	{
		std::unique_lock<std::mutex> lock(mutex_);
		while (WriterEntered || ReaderCount == MaxReaders)
			Reader.wait(lock);
		ReaderCount++;
	}

	void read_unlock()
	{
		std::lock_guard<std::mutex> guard(mutex_);
		ReaderCount--;
		if (WriterEntered)
		{
			if (ReaderCount == 0)
				writer_.notify_one();
		}
		else
		{
			if (ReaderCount == MaxReaders - 1)
				Reader.notify_one();
		}
	}

 private:
	std::mutex mutex_;
	std::condition_variable writer_;
	std::condition_variable Reader;
	uint32_t ReaderCount;
	bool WriterEntered;
};
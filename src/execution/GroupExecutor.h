#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
enum func
{
	SUM,
	MAX,
	MIN,
	COUNT
};
class GroupExecutor : public AbstractExecutor
{
	std::unique_ptr<AbstractExecutor> prev_;
	std::vector<ColMeta> cols_;
	ColMeta col_;
	bool all = false;  // count(*)中的*
	func func_;
	bool isend_ = false;
	ColType type_;
	Value res;  // res存储最终值 Next()函数
	Value temp;  // temp存储临时值
	size_t cnt = 0;  // count计数数量
	size_t len_;
	size_t offset;

 public:
	GroupExecutor(std::unique_ptr<AbstractExecutor> prev, const Group& group)
	{
		prev_ = std::move(prev);
		if (group.func_name == "COUNT")
		{

			func_ = COUNT;
			len_ = 4;
			type_ = TYPE_INT;
			col_.name = group.col.col_name;
			col_.tab_name = group.col.tab_name;
			col_.len = 4;
			col_.type = TYPE_INT;
			col_.offset = 0;
			cols_.push_back(col_);
			all = group.all;
		}
		else
		{
			auto &cols = prev_->cols();
			auto pos = get_col(cols, group.col);
			col_ = *pos;
			offset = col_.offset;
			col_.offset = 0;
			cols_.push_back(col_);
			type_ = col_.type;
			len_ = col_.len;

			if (group.func_name == "MAX")
				func_ = MAX;
			else if (group.func_name == "MIN")
				func_ = MIN;
			else if (group.func_name == "SUM")
				func_ = SUM;
		}
	}
	void beginTuple() override
	{
		prev_->beginTuple();
		if (prev_->is_end())
		{
			switch (type_)
			{
			case TYPE_INT:
				res.set_int(0);
				break;
			case TYPE_FLOAT:
				res.set_float(0.0);
				break;
			case TYPE_BIGINT:
				res.set_bigint(0);
				break;
			default:
				break;
			}
			return;
		}
		// all 表示 count(*) 中的*
		if (all)
		{
			work();
			return;
		}
		// cnt自增, 并用beginTuple初始化第一个值, 以便work()函数比较值
		if (func_ == COUNT)
			cnt += 1;
		else
		{
			auto rec = prev_->Next();
			char* rec_buf = rec->data + offset;
			switch (type_)
			{
			case TYPE_INT:
				res.set_int(*(int*)rec_buf);
				break;
			case TYPE_FLOAT:
				res.set_float(*(float*)rec_buf);
				break;
			case TYPE_BIGINT:
				res.set_bigint(*(int64_t*)rec_buf);
				break;
			case TYPE_DATETIME:
				res.set_datetime(*(int64_t*)rec_buf);
				break;
			default:
				break;
			}
			if (type_ == TYPE_STRING)
			{
				auto col_str = std::string((char*)rec_buf, col_.len);
				col_str.resize(strlen(col_str.c_str()));
				res.set_str(col_str);
			}
		}
		prev_->nextTuple();
		work();
	}
	void nextTuple() override
	{
		isend_ = true;
	}
	[[nodiscard]] const std::vector<ColMeta>& cols() const override
	{
		return cols_;
	};
	[[nodiscard]] bool is_end() const override
	{
		return isend_;
	};
	Rid& rid() override
	{
		return _abstract_rid;
	}
	std::unique_ptr<RmRecord> Next() override
	{
		auto r = std::make_unique<RmRecord>(len_);
		if (func_ == COUNT)
		{
			*(int*)(r->data) = cnt;
			return r;
		}
		if (type_ == TYPE_INT)
		{
			*(int*)(r->data) = res.int_val;
		}
		else if (type_ == TYPE_BIGINT)
		{
			*(int64_t*)(r->data) = res.bigint_val;
		}
		else if (type_ == TYPE_DATETIME)
		{
			*(int64_t*)(r->data) = res.datetime_val;
		}
		else if (type_ == TYPE_FLOAT)
		{
			*(float*)(r->data) = res.float_val;
		}
		else if (type_ == TYPE_STRING)
		{
			if (len_ < (int)res.str_val.size())
			{
				throw StringOverflowError();
			}
			memset(r->data, 0, len_);
			memcpy(r->data, res.str_val.c_str(), res.str_val.size());
		}
		return r;
	}
	void work()
	{
		while (!prev_->is_end())
		{
			// 聚合函数为count时
			if (func_ == COUNT)
			{
				cnt += 1;
				prev_->nextTuple();
				continue;
			}
			// 聚合函数为min/max/sum时
			auto rec = prev_->Next();
			// temp用以保存临时值
			char* rec_buf = rec->data + offset;
			switch (type_)
			{
			case TYPE_INT:
				temp.set_int(*(int*)rec_buf);
				break;
			case TYPE_FLOAT:
				temp.set_float(*(float*)rec_buf);
				break;
			case TYPE_BIGINT:
				temp.set_bigint(*(int64_t*)rec_buf);
				break;
			case TYPE_DATETIME:
				temp.set_datetime(*(int64_t*)rec_buf);
				break;
			default:
				break;
			}
			if (type_ == TYPE_STRING)
			{
				auto col_str = std::string((char*)rec_buf, col_.len);
				col_str.resize(strlen(col_str.c_str()));
				temp.set_str(col_str);
			}
			// temp与res进行比较
			switch (func_)
			{
			case MIN:
				if (res>temp)std::swap(res,temp);
				break;
			case MAX:
				if (res<temp)std::swap(res,temp);
				break;
			case SUM:
				res += temp;
				break;
			default:
				break;
			}
			prev_->nextTuple();
		}
	}
};
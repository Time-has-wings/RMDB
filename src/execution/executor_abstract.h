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

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor
{
 public:
	Rid _abstract_rid{};

	Context* context_{};

	virtual ~AbstractExecutor() = default;

	[[nodiscard]] virtual size_t tupleLen() const
	{
		return 0;
	};

	[[nodiscard]] virtual const std::vector<ColMeta>& cols() const
	{
	};

	virtual std::string getType()
	{
		return "AbstractExecutor";
	};

	virtual void beginTuple()
	{
	};

	virtual void nextTuple()
	{
	};

	[[nodiscard]] virtual bool is_end() const
	{
		return true;
	};

	virtual Rid& rid() = 0;

	virtual std::unique_ptr<RmRecord> Next() = 0;

	virtual ColMeta get_col_offset(const TabCol& target)
	{
		return {};
	};

	static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta>& rec_cols, const TabCol& target)
	{
		auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta& col)
		{ return col.tab_name == target.tab_name && col.name == target.col_name; });
		if (pos == rec_cols.end())
		{
			throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
		}
		return pos;
	}
	static Value get_value(const RmRecord& record, const ColMeta& col)
	{
		char* data = record.data + col.offset;
		size_t len = col.len;
		Value ret;
		if (col.type == TYPE_INT)
			ret.set_int(*(int*)data);
		else if (col.type == TYPE_FLOAT)
			ret.set_float(*(float*)data);
		else if (col.type == TYPE_BIGINT)
			ret.set_bigint(*(int64_t*)data);
		else if (col.type == TYPE_DATETIME)
			ret.set_datetime(*(int64_t*)data);
		else if (col.type == TYPE_STRING)
		{
			std::string tmp(data, len);
			ret.set_str(tmp);
		}
		return ret;
	}
	static void set_value(const RmRecord& record, const ColMeta& col, Value& val)
	{
		char* data = record.data + col.offset;
		size_t len = col.len;
		if (col.type == TYPE_INT)
			val.set_int(*(int*)data);
		else if (col.type == TYPE_FLOAT)
			val.set_float(*(float*)data);
		else if (col.type == TYPE_BIGINT)
			val.set_bigint(*(int64_t*)data);
		else if (col.type == TYPE_DATETIME)
			val.set_datetime(*(int64_t*)data);
		else if (col.type == TYPE_STRING)
		{
			std::string tmp(data, len);
			val.set_str(tmp);
		}
	}

	static bool compare_value(const Value& left_value, const Value& right_value, CompOp op)
	{
		if (left_value.incompatible_type_compare(right_value))
		{
			throw IncompatibleTypeError(coltype2str(left_value.type), coltype2str(right_value.type));
		}
		switch (op)
		{
		case OP_EQ:
			return left_value == right_value;
		case OP_NE:
			return left_value != right_value;
		case OP_LT:
			return left_value < right_value;
		case OP_GT:
			return left_value > right_value;
		case OP_LE:
			return left_value <= right_value;
		case OP_GE:
			return left_value >= right_value;
		}
		return false;
	}
};
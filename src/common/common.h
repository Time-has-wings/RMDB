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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"
#include "sourcelib/datetime.h"
struct TabCol
{
    std::string tab_name;
    std::string col_name;
    std::string as_name;
    bool isGroup = false;
    friend bool operator<(const TabCol &x, const TabCol &y)
    {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};

struct Value
{
    ColType type; // type of value
    union
    {
        int int_val;     // int value
        float float_val; // float value
        int64_t bigint_val;
        int64_t datetime_val; // datetime
        bool invalid_val;
    };

    std::string str_val;           // string value
    std::shared_ptr<RmRecord> raw; // raw record buffer
    bool incompatible_type_compare(const Value &b) const
    {
        return (this->type == TYPE_STRING && b.type != TYPE_STRING) || (this->type != TYPE_STRING && b.type == TYPE_STRING);
    }
    void set_int(int int_val_)
    {
        type = TYPE_INT;
        int_val = int_val_;
    }
    void set_invalidVal(bool invalid_val_)
    {
        type = TYPE_INVALID;
        invalid_val = invalid_val_;
    }
    void set_bigint(int64_t bigint_val_)
    {
        type = TYPE_BIGINT;
        bigint_val = bigint_val_;
    }
    void set_float(float float_val_)
    {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_)
    {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }
    void set_datetime(std::string datetime_val_)
    {
        type = TYPE_DATETIME;
        datetime_val = datetime::datetime_trans(datetime_val_);
    }
    void set_datetime(int64_t datetime_val_)
    {
        type = TYPE_DATETIME;
        datetime_val = datetime_val_;
    }
    void init_raw(int len)
    {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT)
        {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        }
        else if (type == TYPE_BIGINT)
        {
            assert(len == sizeof(int64_t));
            *(int64_t *)(raw->data) = bigint_val;
        }
        else if (type == TYPE_DATETIME)
        {
            assert(len == sizeof(int64_t));
            *(int64_t *)(raw->data) = datetime_val;
        }
        else if (type == TYPE_INVALID)
        {
            return;
        }
        else if (type == TYPE_FLOAT)
        {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        }
        else if (type == TYPE_STRING)
        {
            if (len < (int)str_val.size())
            {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }
    void value_change(const ColType &Type)
    {
        if (type == TYPE_INT)
        {
            if (Type == TYPE_BIGINT)
            {
                set_bigint(int_val);
            }
            else if (Type == TYPE_FLOAT)
            {
                set_float(int_val);
            }
        }
        else if (type == TYPE_BIGINT)
        {
            if (Type == TYPE_INT)
            {
                throw InternalError("Overflow");
            }
        }
        else if (type == TYPE_DATETIME)
        {
            if (Type == TYPE_STRING)
            {
				set_str(trans_datetime(datetime_val));
            }
        }
    }
    bool operator>(const Value &rhs) const
    {
        switch (type)
        {
        case TYPE_INT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return int_val > rhs.int_val;
            case TYPE_FLOAT:
                return int_val > rhs.float_val;
            case TYPE_BIGINT:
                return int_val > rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_FLOAT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return float_val > rhs.int_val;
            case TYPE_FLOAT:
                return float_val > rhs.float_val;
            case TYPE_BIGINT:
                return float_val > rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_BIGINT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return bigint_val > rhs.int_val;
            case TYPE_FLOAT:
                return bigint_val > rhs.float_val;
            case TYPE_BIGINT:
                return bigint_val > rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_STRING:
            return str_val > rhs.str_val;
        case TYPE_DATETIME:
            return datetime_val > rhs.datetime_val;
        }
        return false;
    }

    bool operator<(const Value &rhs) const
    {
        switch (type)
        {
        case TYPE_INT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return int_val < rhs.int_val;
            case TYPE_FLOAT:
                return int_val < rhs.float_val;
            case TYPE_BIGINT:
                return int_val < rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_FLOAT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return float_val < rhs.int_val;
            case TYPE_FLOAT:
                return float_val < rhs.float_val;
            case TYPE_BIGINT:
                return float_val < rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_BIGINT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return bigint_val < rhs.int_val;
            case TYPE_FLOAT:
                return bigint_val < rhs.float_val;
            case TYPE_BIGINT:
                return bigint_val < rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_STRING:
            return str_val < rhs.str_val;
        case TYPE_DATETIME:
            return datetime_val < rhs.datetime_val;
        }
        return false;
    }

    bool operator==(const Value &rhs) const
    {
        switch (type)
        {
        case TYPE_INT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return int_val == rhs.int_val;
            case TYPE_FLOAT:
                return int_val == rhs.float_val;
            case TYPE_BIGINT:
                return int_val == rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_FLOAT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return float_val == rhs.int_val;
            case TYPE_FLOAT:
                return float_val == rhs.float_val;
            case TYPE_BIGINT:
                return float_val == rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_BIGINT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return bigint_val == rhs.int_val;
            case TYPE_FLOAT:
                return bigint_val == rhs.float_val;
            case TYPE_BIGINT:
                return bigint_val == rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_STRING:
            return str_val == rhs.str_val;
        case TYPE_DATETIME:
            return datetime_val == rhs.datetime_val;
        }
        return false;
    }
    bool operator!=(const Value &rhs) const
    {
        switch (type)
        {
        case TYPE_INT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return int_val != rhs.int_val;
            case TYPE_FLOAT:
                return int_val != rhs.float_val;
            case TYPE_BIGINT:
                return int_val != rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_FLOAT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return float_val != rhs.int_val;
            case TYPE_FLOAT:
                return float_val != rhs.float_val;
            case TYPE_BIGINT:
                return float_val != rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_BIGINT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return bigint_val != rhs.int_val;
            case TYPE_FLOAT:
                return bigint_val != rhs.float_val;
            case TYPE_BIGINT:
                return bigint_val != rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_STRING:
            return str_val != rhs.str_val;
        case TYPE_DATETIME:
            return datetime_val != rhs.datetime_val;
        }
        return false;
    }

    bool operator>=(const Value &rhs) const
    {
        switch (type)
        {
        case TYPE_INT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return int_val >= rhs.int_val;
            case TYPE_FLOAT:
                return int_val >= rhs.float_val;
            case TYPE_BIGINT:
                return int_val >= rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_FLOAT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return float_val >= rhs.int_val;
            case TYPE_FLOAT:
                return float_val >= rhs.float_val;
            case TYPE_BIGINT:
                return float_val >= rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_BIGINT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return bigint_val >= rhs.int_val;
            case TYPE_FLOAT:
                return bigint_val >= rhs.float_val;
            case TYPE_BIGINT:
                return bigint_val >= rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_STRING:
            return str_val >= rhs.str_val;
        case TYPE_DATETIME:
            return datetime_val >= rhs.datetime_val;
        }
        return false;
    }

    bool operator<=(const Value &rhs) const
    {
        switch (type)
        {
        case TYPE_INT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return int_val <= rhs.int_val;
            case TYPE_FLOAT:
                return int_val <= rhs.float_val;
            case TYPE_BIGINT:
                return int_val <= rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_FLOAT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return float_val <= rhs.int_val;
            case TYPE_FLOAT:
                return float_val <= rhs.float_val;
            case TYPE_BIGINT:
                return float_val <= rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_BIGINT:
            switch (rhs.type)
            {
            case TYPE_INT:
                return bigint_val <= rhs.int_val;
            case TYPE_FLOAT:
                return bigint_val <= rhs.float_val;
            case TYPE_BIGINT:
                return bigint_val <= rhs.bigint_val;
            default:
                return false;
            }
            break;
        case TYPE_STRING:
            return str_val <= rhs.str_val;
        case TYPE_DATETIME:
            return datetime_val <= rhs.datetime_val;
        }
        return false;
    }
    void operator+=(const Value &rhs)
    {
        switch (type)
        {
        case TYPE_INT:
            int_val += rhs.int_val;
            break;
        case TYPE_FLOAT:
            float_val += rhs.float_val;
            break;
        default:
            break;
        }
    }
};

enum CompOp
{
    OP_EQ,
    OP_NE,
    OP_LT,
    OP_GT,
    OP_LE,
    OP_GE
};

struct Condition
{
    TabCol lhs_col;  // left-hand side column
    CompOp op;       // comparison operator
    bool is_rhs_val; // true if right-hand side is a value (not a column)
    TabCol rhs_col;  // right-hand side column
    Value rhs_val;   // right-hand side value
};

struct SetClause
{
    TabCol lhs;
    Value rhs;
};
struct Group
{
    TabCol col;
    std::string func_name;
    bool all = false;
};

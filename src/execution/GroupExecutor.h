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
    std::vector<ColMeta> cols_; // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    ColMeta col_;
    bool all = false;
    func func_;
    bool isend_ = false;
    ColType type_;
    Value res;
    Value temp;
    size_t cnt = 0;
    size_t len_;
    size_t offset;

public:
    GroupExecutor(std::unique_ptr<AbstractExecutor> prev, Group group)
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
            cols_.push_back(col_);
            all = group.all;
        }
        else
        {
            auto cols = prev_->cols();
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
            return;
        if (all)
        {
            work();
            return;
        }
        cnt += 1;
        auto rec = prev_->Next();
        char *rec_buf = rec->data + offset;
        switch (type_)
        {
        case TYPE_INT:
            res.set_int(*(int *)rec_buf);
            break;
        case TYPE_FLOAT:
            res.set_float(*(float *)rec_buf);
            break;
        case TYPE_BIGINT:
            res.set_bigint(*(int64_t *)rec_buf);
            break;
        case TYPE_DATETIME:
            res.set_datetime(*(int64_t *)rec_buf);
            break;
        default:
            break;
        }
        if (type_ == TYPE_STRING)
        {
            auto col_str = std::string((char *)rec_buf, col_.len);
            col_str.resize(strlen(col_str.c_str()));
            res.set_str(col_str);
        }
        prev_->nextTuple();
        work();
    }
    void nextTuple() override
    {
        isend_ = true;
    }
    const std::vector<ColMeta> &cols() const
    {
        return cols_;
    };
    bool is_end() const override
    {
        return isend_;
    };
    Rid &rid() override { return _abstract_rid; }
    std::unique_ptr<RmRecord> Next() override
    {
        auto r = std::make_unique<RmRecord>(len_);
        if (func_ == COUNT)
        {
            *(int *)(r->data) = cnt;
            return std::move(r);
        }
        if (type_ == TYPE_INT)
        {
            *(int *)(r->data) = res.int_val;
        }
        else if (type_ == TYPE_BIGINT)
        {
            *(int64_t *)(r->data) = res.bigint_val;
        }
        else if (type_ == TYPE_DATETIME)
        {
            *(int64_t *)(r->data) = res.datetime_val;
        }
        else if (type_ == TYPE_FLOAT)
        {
            *(float *)(r->data) = res.float_val;
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
            if (func_ == COUNT)
            {
                cnt += 1;
                prev_->nextTuple();
                continue;
            }
            auto rec = prev_->Next();
            char *rec_buf = rec->data + offset;
            switch (type_)
            {
            case TYPE_INT:
                temp.set_int(*(int *)rec_buf);
                break;
            case TYPE_FLOAT:
                temp.set_float(*(float *)rec_buf);
                break;
            case TYPE_BIGINT:
                temp.set_bigint(*(int64_t *)rec_buf);
                break;
            case TYPE_DATETIME:
                temp.set_datetime(*(int64_t *)rec_buf);
                break;
            default:
                break;
            }
            if (type_ == TYPE_STRING)
            {
                auto col_str = std::string((char *)rec_buf, col_.len);
                col_str.resize(strlen(col_str.c_str()));
                temp.set_str(col_str);
            }
            switch (func_)
            {
            case MIN:
                res = res < temp ? res : temp;
                break;
            case MAX:
                res = res > temp ? res : temp;
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
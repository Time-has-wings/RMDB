
#include "stdint.h"
#include "errors.h"
#include <string>
void strim(std::string &, const char);
bool check_invalidate(std::string &s);
bool IsLeapYear(int);
bool CheckDate(int, int, int);
struct datetime
{
    static int64_t datetime_trans(std::string val_)
    {
        std::string temp = std::string(val_);
        strim(temp, ':');
        strim(temp, '-');
        strim(temp, ' ');
        if (check_invalidate(temp))
            throw InvalidValueCountError();
        return std::atoll(temp.c_str());
    }
    static std::string trans_datetime(int64_t val_)
    {
        std::string s = std::to_string(val_);
        s.insert(4, 1, '-');
        s.insert(7, 1, '-');
        s.insert(10, 1, ' ');
        s.insert(13, 1, ':');
        s.insert(16, 1, ':');
        return s;
    }
};

#include "datetime.h"
void strim(std::string &str, const char c)
{
    int index = 0;
    if (!str.empty())
    {
        while ((index = str.find(c, index)) != std::string::npos)
        {
            str.erase(index, 1);
        }
    }
}
bool IsLeapYear(int year)
{
    if ((year % 400 == 0) || (year % 4 == 0 && year % 100))
        return true;
    else
        return false;
}

bool CheckDate(int year, int month, int day)
{
    static int mon[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year < 1000 || year > 9999 || month <= 0 || day <= 0)
        return false;
    if (IsLeapYear(year))
        mon[2]++;
    if (1 <= month && month <= 12 && 1 <= day && day <= mon[month])
        return true;
    else
        return false;
}
bool check_invalidate(std::string &s)
{
    if (s.size() != 14)
        return true;
    int year = atoi((s.substr(0, 4)).c_str());
    if (year > 9999 || year < 1000)
        return true;
    int month = atoi((s.substr(4, 2)).c_str());
    int day = atoi((s.substr(6, 2)).c_str());
    if (!CheckDate(year, month, day))
        return true;
    int time = atoi((s.substr(8, 2)).c_str());
    if (time >= 24)
        return true;
    time = atoi((s.substr(10, 2)).c_str());
    if (time > 60)
        return true;
    time = atoi((s.substr(12, 2)).c_str());
    if (time > 60)
        return true;
    return false;
};
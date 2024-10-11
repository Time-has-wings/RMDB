#include "datetime.h"
#include "algorithm"
#include "fmt/format.h"

/**
 * @description: 去除字符串str中的所有字符c
*/
void strim(std::string& str, const char c)
{
	str.erase(std::remove(str.begin(), str.end(), c), str.end());
}

/**
 * @description: 是否为闰年
*/
bool IsLeapYear(int year)
{
	if ((year % 400 == 0) || (year % 4 == 0 && year % 100))
		return true;
	else
		return false;
}


/**
 * @description: 检查year/month/day的合法性
*/
bool CheckDate(int year, int month, int day)
{
	int mon[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
	if (year < 1000 || year > 9999 || month <= 0 || day <= 0)
		return false;
	if (IsLeapYear(year))
		mon[2]++;
	if (month <= 12 && day <= mon[month])
		return true;
	else
		return false;
}

/**
 * @description: 判断string类型表示下的date是否不合法 并返回date的int型表示 
*/
int64_t check_invalidate(std::string& s)
{
	if (s.size() != 14)
		return -1;
	int64_t num = std::strtoll(s.c_str(), nullptr, 10);
	int year = num / 10000000000;
	int month = num / 100000000 % 100;
	int day = num / 1000000 % 100;
	if (!CheckDate(year, month, day))
		return -1;
	int time = num / 10000 % 100;
	if (time > 23)
		return -1;
	time = num / 100 % 100;
	if (time > 59)
		return -1;
	time = num % 100;
	if (time > 59)
		return -1;
	return num;
}

/**
 * @description: datetime类型: from int64 to string
*/
std::string trans_datetime(int64_t val_)
{
	std::string s = fmt::to_string<int64_t>(val_);
	s.insert(4, 1, '-');
	s.insert(7, 1, '-');
	s.insert(10, 1, ' ');
	s.insert(13, 1, ':');
	s.insert(16, 1, ':');
	return s;
}

/**
 * @description: datetime类型: from string to int64
*/
int64_t datetime_trans(const char* s, size_t size)
{
	std::string str(s, size);
	strim(str, ':');
	strim(str, '-');
	strim(str, ' ');
	return check_invalidate(str);
}
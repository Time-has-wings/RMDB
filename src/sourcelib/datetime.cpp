#include "datetime.h"
#include "algorithm"
#include "fmt/format.h"
void strim(std::string& str, const char c)
{
	str.erase(std::remove(str.begin(), str.end(), c), str.end());
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
	static int mon[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
	if (year < 1000 || year > 9999 || month <= 0 || day <= 0)
		return false;
	if (IsLeapYear(year))
		mon[2]++;
	if (month <= 12 && day <= mon[month])
		return true;
	else
		return false;
}
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
int64_t datetime_trans(const char* s, size_t size)
{
	std::string str(s, size);
	strim(str, ':');
	strim(str, '-');
	strim(str, ' ');
	return check_invalidate(str);
}
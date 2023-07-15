//
// Created by Pygon on 2023/7/15.
//

#include "LoadData.h"
#include "common/common.h"
#include "errors.h"
#include <cstdint>
#include <string>
std::regex int_num("[+-]?\\d{1,9}");
std::regex big_num("[+-]?(\\d){10,19}");
std::regex float_num("[+-]?\\d+[.](\\d+)?");
std::regex date_time("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
std::regex str("^\\a");
Value LoadData::trans(std::string &s)
{
	Value res;
	if (std::regex_match(s, str))
	{
		res.set_str(s);
	}
	else if (std::regex_match(s, int_num))
	{
		int num = std::stoi(s);
		res.set_int(num);
	}
	else if (std::regex_match(s, float_num))
	{
		res.set_float(std::stof(s));
	}
	else if (std::regex_match(s, big_num))
	{
		int64_t temp = atoll(s.c_str());
		if (temp <= INT32_MAX && temp >= INT32_MIN)
		{
			res.set_int((int)temp);
		}
		if ((temp == INT64_MAX && strcmp(s.c_str(), "9223372036854775807") != 0) || (temp == INT64_MIN && strcmp(s.c_str(), "-9223372036854775808") != 0))
		{
			throw InternalError("Overflow");
		}
		res.set_bigint(temp);
	}
	else if (std::regex_match(s, date_time))
	{
		res.set_datetime(s);
	}
	else
	{
		res.set_str(s);
	}
	return res;
}

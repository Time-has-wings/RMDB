//
// Created by Pygon on 2023/7/15.
//

#include "LoadData.h"
#include "common/common.h"
#include "errors.h"
#include <charconv>
#include <cstdint>
#include <string>
std::regex int_num("[+-]?\\d{1,9}");
std::regex big_num("[+-]?(\\d){10,19}");
std::regex float_num("[+-]?\\d+[.](\\d+)?");
std::regex date_time("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
std::regex str("^\\a");
Value LoadData::trans(std::string& s)
{
	Value res;
	if (std::regex_match(s, str))
	{
		res.set_str(s);
	}
	else if (std::regex_match(s, int_num))
	{
		int num;
		std::from_chars(s.c_str(), s.c_str() + s.size(), num);
		res.set_int(num);
	}
	else if (std::regex_match(s, float_num))
	{
		res.set_float(std::stof(s));
	}
	else if (std::regex_match(s, big_num))
	{
		int64_t num;
		auto [ptr, ec] = std::from_chars(s.c_str(), s.c_str() + s.size(), num);
		if (num > INT32_MIN && num < INT32_MAX)
		{
			res.set_int((int)num);
		}
		else if (ec != std::errc::result_out_of_range)
			res.set_bigint(num);
		else throw InternalError("Overflow");
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


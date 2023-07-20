//
// Created by Pygon on 2023/7/15.
//

#include "LoadData.h"
#include "common/common.h"
#include "errors.h"
#include <cstdint>
#include <string>
void LoadData::trans(std::string &s, ColMeta &col, char *desrc, size_t off_set)
{
<<<<<<< src/sourcelib/LoadData.cpp
	char t[col.len];
	switch (col.type)
	{
	case TYPE_INT:
		*(int *)t = std::stoi(s);
		break;
	case TYPE_DATETIME:
		*(int64_t *)t = datetime::datetime_trans(s);
		break;
	case TYPE_BIGINT:
		*(int64_t *)t = atoll(s.c_str());
		break;
	case TYPE_STRING:
		memcpy(desrc + off_set, s.c_str(), s.size());
		return;
	case TYPE_FLOAT:
		*(float *)t = std::stof(s);
=======
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
Value LoadData::trans(std::string &s, ColMeta &col)
{
	Value val;
	switch (col.type)
	{
	case TYPE_INT:
		val.set_int(std::stoi(s));
		break;
	case TYPE_DATETIME:
		val.set_datetime(s);
		break;
	case TYPE_BIGINT:
		val.set_bigint(atoll(s.c_str()));
		break;
	case TYPE_STRING:
		val.set_str(s);
		break;
	case TYPE_FLOAT:
		val.set_float(std::stof(s));
>>>>>>> src/sourcelib/LoadData.cpp
		break;
	default:
		break;
	}
<<<<<<< src/sourcelib/LoadData.cpp
	memcpy(desrc + off_set, t, col.len);
=======
	return val;
>>>>>>> src/sourcelib/LoadData.cpp
}
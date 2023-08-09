#pragma once
#include <cstdint>
#include "errors.h"
#include <string>

void strim(std::string&, char);

int64_t check_invalidate(std::string& s);

bool IsLeapYear(int);

bool CheckDate(int, int, int);

std::string trans_datetime(int64_t val_);

int64_t datetime_trans(const char* str, size_t size);

struct datetime
{
	static int64_t datetime_trans(std::string& val_)
	{
		strim(val_, ':');
		strim(val_, '-');
		strim(val_, ' ');
		int64_t res = check_invalidate(val_);
		if (res == -1)
			throw InvalidValueCountError();
		return res;
	}
};
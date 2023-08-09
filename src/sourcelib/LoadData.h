//
// Created by Pygon on 2023/7/15.
//
#pragma once
#include "common/common.h"
#include "errors.h"
#include <regex>
#include <string>
#include <fstream>
#include <iostream>
#include "system/sm_meta.h"
#include <utility>
#include <vector>
class LoadData
{
	std::string file_name;
	std::string tab_name;
	std::vector<std::string> column_names;

 public:
	LoadData(std::string file_name_, std::string tab_name_)
		: file_name(std::move(file_name_)), tab_name(std::move(tab_name_))
	{
	}
	static void trans(const char* s, ColMeta& col, char* desrc, size_t size);
};
int get_int(const char* s, size_t size);
int64_t get_bigint(const char* s, size_t size);
float get_float(const char* str, size_t size);

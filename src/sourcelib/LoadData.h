//
// Created by Pygon on 2023/7/15.
//
#include "common/common.h"
#include "errors.h"
#include <regex>
#include <string>
#include <fstream>
#include <iostream>
#include "system/sm_meta.h"
#include <vector>
extern std::regex int_num;
extern std::regex big_num;
extern std::regex float_num;
extern std::regex date_time;
extern std::regex str;
class LoadData
{
	std::string file_name;
	std::string tab_name;
	std::vector<std::string> column_names;

public:
	LoadData(std::string file_name_, std::string tab_name_) : file_name(file_name_), tab_name(tab_name_)
	{
	}
	static void trans(std::string &, ColMeta &, char *s, size_t off_set);
};

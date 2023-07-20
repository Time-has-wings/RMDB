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
		memcpy(desrc + off_set, s.c_str(), col.len);
		return;
	case TYPE_FLOAT:
		*(float *)t = std::stof(s);
		break;
	default:
		break;
	}
	memcpy(desrc + off_set, t, col.len);
}
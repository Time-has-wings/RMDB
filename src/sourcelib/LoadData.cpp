//
// Created by Pygon on 2023/7/15.
//

#include "LoadData.h"
#include "common/common.h"
#include "errors.h"
void LoadData::trans(const char* s, ColMeta& col, char* desrc, size_t size)
{
	char* t = desrc + col.offset;
	switch (col.type)
	{
	case TYPE_INT:
		*(int*)t = get_int(s, size);
		break;
	case TYPE_DATETIME:
		*(int64_t*)t = datetime_trans(s, size);
		break;
	case TYPE_BIGINT:
		*(int64_t*)t = get_bigint(s, size);
		break;
	case TYPE_STRING:
		memcpy(t, s, size);
		return;
	case TYPE_FLOAT:
		*(float*)t = get_float(s, size);
		break;
	default:
		break;
	}
}
int get_int(const char* s, size_t size)
{
	bool f = false;
	int res = 0;
	if (s[0] == '-')
		f = true;
	else
		res = s[0] ^ '0';
	for (size_t i = 1; i < size; i++)
	{
		res = res * 10 + (s[i] ^ '0');
	}
	return f ? -res : res;
}
int64_t get_bigint(const char* s, size_t size)
{
	bool f = false;
	int64_t res = 0;
	if (s[0] == '-')
		f = true;
	else
		res = s[0] ^ '0';
	for (size_t i = 1; i < size; i++)
	{
		res = res * 10 + (s[i] ^ '0');
	}
	return f ? -res : res;
}
float get_float(const char* s, size_t size)
{
	float result = 0, under_0 = 0;
	int point_index = 0;
	while (point_index < size)
	{
		if (s[point_index] == '.')
			break;
		++point_index;
	}
	bool f = false;
	if (s[0] == '-')
		f = true;
	else
		result = s[0] ^ '0';
	for (int i = 1; i <= point_index - 1; i++)
	{
		result = result * 10 + (s[i] ^ 48);
	}
	for (int i = size - 1; i >= point_index; i--)
	{
		if (i == point_index)
		{
			under_0 = under_0 * 0.1;
		}
		else
		{
			under_0 = under_0 * 0.1 + (s[i] ^ 48);
		}
	}
	result = result + under_0;
	return f ? -result : result;
}

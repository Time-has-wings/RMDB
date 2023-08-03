#include "to_string.h"
#include "fmt/format.h"
std::string To_string(const char* src, const ColMeta& col)
{
	std::string s;
	switch (col.type)
	{
	case TYPE_INT:
		return fmt::to_string<int>(*(int*)src);
	case TYPE_FLOAT:
		return std::to_string(*(float*)src);
	case TYPE_BIGINT:
		return fmt::to_string<int64_t>(*(int64_t*)src);
	case TYPE_DATETIME:
		return trans_datetime(*(int64_t*)src);
	case TYPE_STRING:
		return { src, static_cast<size_t>(col.len) };
	default:
		break;
	}
}
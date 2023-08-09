/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "ix_defs.h"
#include "ix_index_handle.h"

// class IxIndexHandle;

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上读锁
class IxScan : public RecScan
{
	const IxIndexHandle* ih_;
	Iid iid_; // 初始为lower（用于遍历的指针）
	Iid end_; // 初始为upper
	BufferPoolManager* bpm_;
	IxNodeHandle* node_; // important-> 复用

 public:
	IxScan(const IxIndexHandle* ih, const Iid& lower, const Iid& upper, BufferPoolManager* bpm)
		: ih_(ih), iid_(lower), end_(upper), bpm_(bpm)
	{
		node_->page->RLatch();
		node_ = ih_->fetch_node(iid_.page_no); // 获得索引文件的第一个结点
	}

	void next() override;

	[[nodiscard]] bool is_end() const override
	{
		return iid_ == end_;
	}

	[[nodiscard]] Rid rid() const override;

	[[nodiscard]] const Iid& iid() const
	{
		return iid_;
	}

	~IxScan() override
	{
		bpm_->unpin_page(node_->get_page_id(), false);
		node_->page->RUnlatch();
		delete node_;
	}
};
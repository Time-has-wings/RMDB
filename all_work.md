1.scan算子设置标志位(已全部修改完成)
2.update,delete算子设置为对所有record上X锁(已完成)
3.lock系列函数将lock_IX_on_table牵出,即可wait,随后便要修改lock_S_on_table和lock_X_on_table(qwq)
4.insert算子考虑上锁
    4.1 增加数据结构,insert可对record上X锁(增加数据结构是因为insert和delete均会对bitmap产生变化,随后可能会影响scan算子)
    4.2 若考虑4.1比较麻烦,可以尝试如下更改
        4.2.1 将delete算子设置为对table上X锁(原因:性能测试的delete语句较少 && delete涉及多个record而insert只涉及一个record)
        4.2.2 将insert算子修改为对record上X锁
            4.2.2.0 首先判断能不能对table上IX锁
            4.2.2.1 首先寻找空闲槽位
            4.2.2.2 空闲槽位一定没有被其他事务锁住,可以直接插入 
            4.2.2.3 对该槽位上锁,以防update算子
        4.2.3 因delete改为对table上X锁,故而scan算子的标志位将改为int型(具体实现可为枚举变量),
              以区分select delete update, 分别对table上S,X,IX锁
            
pmt是在pebble基础上的增强, 大量使用goto跳过一部分原有逻辑

```
- 增加Part和PartIdx
把键范围切成若干Part
每个Part都是{key,files,..}
用SstMap记录SST的 (Smallest,Largest,Size)

- 修改OutputSplitter
原来, 依据grandparent-overlap
现在, 依据区间边界
边界来自区间索引

- 增加广义多层压实
关闭一些Pebble的默认行为: 自动压实和移动压实
注释掉db.flush中的maybeScheduleCompaction
修改compaction迭代器, 增加memtable

- 新增SSTable格式
TableFormatPMT0
4KB DataPage + IndexPage+ footer
RawWriter适配
不过把compaction输出切到PMT0的开关目前还是注释状态
```

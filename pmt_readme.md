pmt是在pebble基础上的增强, 大量使用goto跳过一部分原有逻辑

```
- 引入 Part&PartIdx
把 key space 切成若干Part
每个Part维护一个 Stack
用SstMap记录SST的 (Smallest,Largest,Size)

- 修改 OutputSplitter

- SST不跨分区
原Pebble依据grandparent-overlap的split
换成分区边界的split
边界来自PartIdx[i].High+1

- 改compaction/flush的迭代器
memtable+ 各level文件

- 关闭一些Pebble的默认行为
flush后不自动compaction
关闭move compaction

- 新增SSTable格式
TableFormatPMT0
4KB DataPage + IndexPage+ footer
RawWriter适配
不过把compaction输出切到PMT0的开关目前还是注释状态
```

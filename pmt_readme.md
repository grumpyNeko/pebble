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

# TableFormatPebblev6比v1小   
columnarblock，把key/trailer/value分列编码，固定长度数据省开销
  Header                                                                                                                                                                                                                                          
  列1: key_prefix[]      (一列全是前缀)                                                                                                                                                                                                           
  列2: key_suffix[]      (一列全是后缀)                                                                                                                                                                                                           
  列3: trailer[]         (seqnum+kind)                                                                                                                                                                                                            
  列4: value[]                                                                                                                                                                                                                                    
  列5: isValueExternal[] (位图)                                                                                                                                                                                                                   
  列6: isObsolete[]      (位图)                                                                                                                                                                                                                   
  列7: prefixChanged[]   (位图)                                                                                                                                                                                                                   
  Padding      

# 新增TableFormatPMT0
只支持SET, key/value都是uint64
+---------------------------+
|        DataPage           |
+---------------------------+
|          ...              |
+---------------------------+
|        DataPage           |
+---------------------------+
|        IndexPage          |
+---------------------------+
IndexPage, 4KB, userkey的有序数组
DataPage, 4KB, {userkey,val}的有序数组

RawWriter适配
通过EnablePMTTableFormat开关
没接入Pebble read path: DB.Get -> getIter -> newIters -> sstable.NewReader

Options增加字段FileFormat
在DB.TableFormat()中优先使用opts.FileFormat, 原先取FormatMajorVersion再按实验开关降级

在my.go中实现PMT专门的查找PMTGet(k)
  PartIdx + SstMap + Stack
  返回(v, found, tableCt)
```


# 改动






# Part和PartIdx
把键范围切成若干Part
每个Part都是{key,files,..}
用SstMap记录SST的 (Smallest,Largest,Size)

# 修改OutputSplitter
原来, 依据grandparent-overlap
现在, 依据区间边界
边界来自区间索引

# 增加广义多层压实
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

直接设置FileFormat, 不止为了PMT, 也方便对比其他tableformat
在Options中增加了字段FileFormat // 确实侵入了
在DB.TableFormat()中优先使用opts.FileFormat, 原先取FormatMajorVersion再按实验开关降级

# 专门的查找PMTGet(k)
PartIdx + SstMap + Stack
返回(v, found, tableCt)

# pmtformat.Iter用于Get/Compact
SeekGE/First/Next/SeekLT/Prev
SetBounds
Error/Close/SetContext
按Page懒加载
Kind<-InternalKeyKindSet
SeqNum<-FileMetadata.LargestSeqNum
- iter_test.go
空表, First/Last/SeekGE/SeekLT
First->Next、Last->Prev、SeekGE/SeekLT
257 KV, SeekGE页尾, Next到下一页
SeekGE(>max)=nil，SeekLT(<=min)=nil，含SetBounds       
- 在compaction路径接入该迭代器
入口是 compactAndWrite:3237，它调用 c.newInputIters(...) 组装输入迭代器。
newInputIters 里 point 走 newLevelIter(..., newIters, ...)，最终都会落到 fileCacheHandle.newIters:541。
在 PMT 开关下，newIters 直接分流到 newPMTIters:15，这里构造的是 pmtformat.NewIter(...)（懒加载按 page 读）而不是 sstable reader。
newRangeDelIter 单独请求 iterRangeDeletions 时，newPMTIters 不会返回 point，也不会返回 rangedel，所以 newRangeDelIter:1076 会拿到 nil 并跳过。
最后 rangeDelIters/rangeKeyIters 为空，compact.NewIter(cfg, pointIter, nil, nil)，即 point-only compaction。   
- 在DB.Get路径接入该迭代器
DB.Get 初始化 getIter 时注入 newIters: d.newIters，见 db.go:602。
getIter 在 getSSTableIterators:263 调 g.newIters(..., iterPointKeys|iterRangeDeletions)。
PMT 分流后同样进入 newPMTIters:15，point 直接是 pmtformat.Iter，不再经过 pmtTableReader/pmtformat.Reader。

# TableFormatPMT读路径接入BlockCache

其他tableformat涉及sstable.Reader/block.Reader, not pmt
why? pmt没有footer/metaindex/index/properties
没有tableCache和blockCache

pmtformat.NewIter, 用pmtCachedReadable包Readable, 传给pmtformat.NewIter, 增加BlockCache逻辑
- blockCacheHandle.GetWithReadHandle(..)
  IF hit, ..
  IF miss, alloc, read, set, release
  为什么set后要release? ..
- Close, 先关ReadHandle再关Readable
- compaction的读取不缓存：internalOpts.compaction时只走ReadHandle.SetupForCompaction()
- 计数hit/miss?  db.Metrics()..BlockCache.Misses







# Part和PartIdx
把键范围切成若干Part
每个Part都是{key,files,..}
用SstMap记录SST的 (Smallest,Largest,Size)

# CompactionEnd->newPartIdxFrom
``` 
newPartIdxFrom(pList) []Part {
	ret = []
	for p in pList
	  continue if p.Outputs == 0 && p.WriteTo == 0 
    if p.WriteTo == 0 // split old part, one newPart for one output
      sort p.Outputs
      for f in p.Outputs
        ret += {..}
    else 
      ret += {
      	Low:   p.Low,
				High:  p.High,
				Stack: p.Stack[:p.WriteTo] + p.Outputs,
      }
  // last part is sentinel
	if ret.len == 0 
		ret += Part{
			Low:   0,
			High:  MaxUint64,
			Stack: nil,
		})
	if ret[-1].High != MaxUint64 
    ret[-1].High = MaxUint64
  return ret 
}
```

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

# 对比TableFormatLevelDB和TableFormatPMT的随机点查找性能
pmt快很多
在一个单元测试里, 先用现有测试数据构造文件, 随机查询1M次, 统计时间

根据Test_pmt_wa和Test_pebble_wa, 写入128轮normal_plus
一次随机查找, pebble平均查找3.77个文件, pmt平均查找3.9914个文件
16线程并发
random read需12853ms
pmt需16376ms

为什么pmt格式文件本身更快, 但整体上更慢
newPMTIters每次都OpenForReading + NewReadHandle + NewIter
LevelDB走table cache

pmt怎么加tableCache
先findOrCreateTable，复用Readable，不要每次OpenForReading

最小落地改动点? 
- 去掉 openFile 里对 PMT table 的 panic，见 file_cache.go:214。
- 给 fileCacheValue 增加 PMT reader 形态（或通用 Readable 字段），见 file_cache.go:881。                                                                                                                                                     
- newPMTIters 从 cached value 创建 iter，并用 closeHook 做 Unref，避免每次打开文件。     


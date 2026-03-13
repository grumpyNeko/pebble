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
========================================================================================================================
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
API
  SeekGE/First/Next/SeekLT/Prev
  SetBounds
  Error/Close/SetContext
Kind?   InternalKeyKindSet
SeqNum? Iter需要传入一个sn, 此后读出的都是它; 传入的是FileMetadata.LargestSeqNum
FileMetadata.LargestSeqNum? 是写入文件内的InternalKey的最大SeqNum
写入文件内的InternalKey是? 先seqNum=db.mu.versions.logSeqNum.Add(1) - 1，再newSimpleMemIter(keys, v, seqNum)把新数据全设置为同一seqnum
- iter_test.go
空表, First/Last/SeekGE/SeekLT
First->Next、Last->Prev、SeekGE/SeekLT
257 KV, SeekGE页尾, Next到下一页
SeekGE(>max)=nil，SeekLT(<=min)=nil，含SetBounds       
- 影响compactAndWrite和DB.Get
判断是否启用了TableFormatPMT，若是, 调用pmtformat.NewIter(..)而不是sstable reader

# TableFormatPMT读接入BlockCache
其他tableformat涉及sstable.Reader/block.Reader, not pmt
why? pmt没有footer/metaindex/index/properties
所以没有tableCache和blockCache

用pmtCachedReadable包Readable, 传给pmtformat.NewIter, 增加BlockCache逻辑
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
========================================================================================================================
# plan step1 
区间上层文件越小, 新数据越大, 越倾向于重写上层

方案1
rewritePages + nextFilePages <= newPages + b
从上往下遍历Stack, 直到发现i层不满足, 重写i以上的层
```
writeTo = stack.len
for i=stack.len-1; i>=0; i--
  table <- Stack[i]
  break if rewritePages + nextFilePages > a*newPages + b
  writeTo = i
if writeTo == 4
  writeTo--
```
这个方案的问题
newPages:860, writeTo:0, stack: [513]
newPages:1, writeTo:3, stack: [4,4,4,5]
newPages:0, writeTo:0, stack: [459,5,5,5]

方案2
```
n = stack.len
oldCost_i = 0
for j=i+1; j>=n-1; j++
  oldCost_i += (j-i) * tablesize_j
cost_i = r*tablesize_i - w_new * (n-i) - oldCost_i
```
r*tablesize_i表示 重写输出层
w_new * (n-i)表示 新数据
oldCost_i表示     已有的文件

实现方案2为step1V4
修正项: 在重叠度过高时促进上层压实，当n>C且i<T时, 把cost_i减去B

# plan
```
plan(keys)
	flushPlan = planStep1(keys)
	flushPlan = mergeAdjacent_wt0(flushPlan)
	if flushPlan.totalWriteExpected < **
		flushPlan = activeMergePlan(flushPlan, extraWriteThreshold)
		flushPlan = mergeAdjacent_wt0(flushPlan)
	if flushPlan.totalWriteExpected > **
	  flushPlan = delaySmallCompaction(flushPlan)
	return flushPlan

mergeAdjacent_wt0(planList) {
  mergeCount = 0
  newPlanList = []
  for idx,p in planList
    if p.WriteTo != 0
      newPlanList += p
      continue
    if newPlanList[-1].WriteTo != 0 
      newPlanList += p
      continue
    newPlanList[-1] = mergePartPlan(newPlanList[-1], p)
    mergeCount++
  return newPlanList, mergeCount
}

// 提前压实, 促进区间再均衡
activeMergePlan(flushplan) {
	pushList = []
	for ppIdx in flushplan.wt0
	  prev = ..
	  succ = ..
	  push, ok = prev和succ中更小的
	  continue if !ok 
	  extraWrite = 提前压实相比原方案多出来的写入量
	  pushList += {tryPush, extraWrite}
	pushList去重
	pushList按extraWrite升序
	totalExtraWrite = 0
	for {idx, extraWrite} in tryPushList
	  break if totalExtraWrite + extraWrite > **
	  flushplan.wt0 += idx
	  flushplan.plist[idx] <- writeTo=0, reason=advance
	  flushPlan.totalExtraWrite += 提前压实的额外写入量
	  flushplan.activeMergeCount++
	return flushplan
}

// 新数据少就推迟压实, 减少写入峰值
delay(flushPlan) FlushPlan {
	for idx in flushPlan.wt0 
		pp = flushPlan.planList[idx]
		continue if pp.NewPages >= DelayCompactNewPagesThreshold 
		remove idx from flushPlan.wt0 
		pp.WriteTo = newWriteTo
		reducedWrite = ..
		flushPlan.totalWriteExpected -= reducedWrite
		flushPlan.totalReducedWrite += reducedWrite
		flushPlan.delayCompactCount++
	}
	flushPlan.wt0 = collectWt0(flushPlan.planList)
	return flushPlan
}

chooseTryPushIdx(flushPlan, wt0Idx) (int, uint64) {
	bestIdx = -1
	bestWrite = maxuint64
	tryUpdate = (idx int){
		return if idx < 0 || idx >= len(flushPlan.planList) 
		extraWrite = extraWriteExpected(flushPlan.planList[idx])
		if extraWrite < bestWrite 
			bestIdx = idx
			bestWrite = extraWrite

	tryUpdate(wt0Idx - 1)
	tryUpdate(wt0Idx + 1)

	return bestIdx, bestWrite
}
```

提前压实, prev和succ哪个更适合改为writeTo=0?
activeMergePlan, totalExtraWrite, ..很多名字不太好, 统计方式也次优
wt0改成map, 不要多次collectWt0而是随时维护wt0

# stack slot 与 pebble level
outputLevelForWriteTo希望保持stack slot与pebble level一一对应 
- writeTo=0 -> L6  
- writeTo=1 -> L5 
- writeTo=2 -> L4 
但有例外
stack=[L6,L5,L4], writeTo=1, 写入L5, 产生两个文件                               
stack变成[L6,L5,L5], writeTo=2, 写入L4, 但输入中有L5文件
此时slot与Level不是一一对应

怎么办 
- outputLevel=max(outputLevelForWriteTo(writeTo),maxInputLevel), 这不对, pebble的Level只有7层, 改manifest.NumLevels容易出问题
- stack slot改为[]FileNum, 会对其他代码有影响!
- 都放在L0
- 修改输出文件的level, 让slot和level对应

都放在L0?
  设置L0 sublevel数量
  由于pmt的multilevelflush的输出总是最新的, 直接放L0(最上面), 不会导致老数据掩盖新数据(即使没有seqnum)
反对? 删旧L0文件+加新L0文件，重建sublevels

尝试: 删outputLevelForWriteTo, stack writeTo不再映射Pebble level, multiflush只写L0
L0按seqnum排序
panic: L0 files 000140 and 000062 are not properly ordered: <#0-#91> vs <#0-#37>
可能与L0的特殊顺序有关
```go
func newCompactionInputLevelSlice(
	cmp base.Compare, level int, files []*manifest.TableMetadata,
) manifest.LevelSlice {
	if level == 0 {
		return manifest.NewLevelSliceSeqSorted(files)
	}
	return manifest.NewLevelSliceKeySorted(cmp, files)
}
```

目前, 放宽_newPickedFilesCompaction里的检查, 能跑但不知道为什么
- 去掉“outputLevel 不能浅于输入最大层”的限制 
- inputs按实际输入层收集，不按startLevel..outputLevel连续区间收集
- 给adjustedOutputLevel加了下限，避免opts.Level(-1)

# 注意
妥协: collector需要计算新数据量
  step1Simple不计算
  step1V4重复计算

妥协: collectorIter读出的seqnum为0

妥协: collectorIter现在只被包成flushableCollectorIter用作compaction的输入，不在pebble的getIter/readState读路径里

难点: collector影响compactAndWrite, 只是为了更新Version

妥协: MustGet现在不panic

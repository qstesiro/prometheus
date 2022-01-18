# Todo:
{
    - head.Chunk 数据写入与db.Compact关系 ok
    - head.Chunk 写mmap时间并不严格的是超过截断时间的第一个时间 ok
    - metrics 在head中存储是使用memSeries,每个memSeries关联一个chunc(xor)存储采样数据 ok
    - VectorSelector字段含义
    - VectorSelector.Offset如何计算
    - PreprocessExpr
    - 逐个分析Expr ok
    - generic.go中接口,结构分析
    - blockReader/blockQuerier类型分析

    # 二次开发
    - 判定series是否存在与存在但没有采样值两种情况
    - 扩展内置函数
    - 支持简单的select查询
}


git config --local http.proxy http://192.168.56.1:4780
git config --local https.proxy https://192.168.56.1:4780
git config --local https.proxy socks5://192.168.56.1:4781
git config --local https.proxy socks5://192.168.56.1:4781

git config --local --unset http.proxy
git config --local --unset http.proxy

git config --local -l

./prometheus --config.file=documentation/examples/prometheus.yml

# 编译与调试
{
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" -o prometheus cmd/prometheus/main.go'
    # 不删除data
    alias dlv='gob && dlv exec ./prometheus --init .dbg/prometheus.dlv -- --config.file=documentation/examples/prometheus.yml --enable-feature=promql-at-modifier --query.timeout=1h'
    # 删除data
    alias dlv='gob && rm -rf data/ && dlv exec ./prometheus --init .dbg/prometheus.dlv -- --config.file=documentation/examples/prometheus.yml'
}

memChunk/mmappedChunk区别 !!!
if numSamples == samplesPerChunk/4 {
memSeries.head.maxTime !!!
memSeries.sampleBuf[4] !!!
emacs正则 !!!
color配制 !!!
ChunkDiskMapper ???
emacs curor !!!
fileutil ???

hash: 6870465778796250587,

prometheus_target_interval_length_seconds{interval="15s",quantile="0.01"} 11.260815277
prometheus_target_interval_length_seconds{interval="15s",quantile="0.05"} 14.999539874
prometheus_target_interval_length_seconds{interval="15s",quantile="0.5"} 14.999984526
prometheus_target_interval_length_seconds{interval="15s",quantile="0.9"} 15.000614365
prometheus_target_interval_length_seconds{interval="15s",quantile="0.99"} 16.882491995

prometheus_target_interval_length_seconds{} / prometheus_target_interval_length_seconds{}
prometheus_target_interval_length_seconds{quantile="0.5"} / prometheus_target_interval_length_seconds{}
prometheus_target_interval_length_seconds{quantile="0.5"} / ignoring(intervals)  prometheus_target_interval_length_seconds{}
prometheus_target_interval_length_seconds{quantile="0.5"} / ignoring(intervals) group_right prometheus_target_interval_length_seconds{}

go_gc_duration_seconds{quantile=0.5} < go_gc_duration_seconds{quantile=0.75}

(go_gc_duration_seconds_count{} + go_gc_duration_seconds_count{}) @ start()

Queryable.Querier: fanout.Querier -> querierAdapter.Querier(实际调用mergeGenericQuerier.Querier)
readStorage.Querier -> db.Querier -> querierAdapter.Querier
Querier.Select: mergeGenericQuerier.Select -> genericQuerierAdapter.Select -> blockQuerier.Select
SerieSet.At: blockSeriesSet.At
SeriesEntry.Iterator: populateWithDelGenericSeriesIterator.toSeriesIterator
Iterator.At: populateWithDelSeriesIterator.At

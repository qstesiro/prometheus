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

# 编译与调试
{
    # 编译
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" -o prometheus cmd/prometheus/main.go'

    # 不删除data
    alias dlv='gob && dlv exec ./prometheus --init .dbg/prometheus.dlv -- --config.file=documentation/examples/prometheus.yml --enable-feature=promql-at-modifier --query.timeout=1h'

    # 删除data
    alias dlv='gob && rm -rf data/ && dlv exec ./prometheus --init .dbg/prometheus.dlv -- --config.file=documentation/examples/prometheus.yml --enable-feature=promql-at-modifier --query.timeout=1h'

    # 测试
    alias dlv='CGO_ENABLED=0 dlv test github.com/prometheus/prometheus/tsdb --init .dbg/prometheus.dlv -- -test.run TestPostingsForMatchers'
}

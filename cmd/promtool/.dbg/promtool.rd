# 编译与调试
{
    # 首先cd cmd/promtool
    alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" -o promtool'
    alias dlv='go && dlv exec ./promtool --init .dbg/promtool.dlv -- tsdb list'
}

alias gob='CGO_ENABLED=0 go build -v -gcflags "all=-N -l" -o adapter ./main.go'
alias dlv='gob && dlv exec ./adapter --init .dbg/adapter.dlv -- --elastic-url="http://10.138.16.188:5601"'

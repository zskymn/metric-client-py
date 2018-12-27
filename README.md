# metric-client

metric client for python

## Install

```sh
pip install metric-client
```

## Usage

```python
from metric_client import MetricClient

token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXJzaW9uIjoxLCJhcHBjb2RlIjoib3BzX21ldHJpY2d3IiwidG9fdXNlciI6Inh4eHguemhhbyIsImlhdCI6MTU0NTczNTczMH0.Aj8srWIjyFwxhcMrZlCxyNlP44uLG0iiR31ynyYd4Bw'
send_api = 'http://localhost:6066/v1/metric/send'
metric = MetricClient(send_api, token)

# send_api: 接收推送数据的API
# token：负责appcode的身份验证，不能为空
```
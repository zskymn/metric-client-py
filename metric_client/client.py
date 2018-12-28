# coding=utf-8

import sys
import threading
import time

import requests
from qtdigest import Tdigest

PY2 = sys.version_info.major == 2


class MetricClient(object):
    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._instance_lock:
                cls._instance = super(MetricClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, send_api, token):
        self.send_api = self._check_not_empty_string(send_api, 'send_api')
        self.token = self._check_not_empty_string(token, 'token')

        self.set_metrics = {}
        self.counter_metrics = {}
        self.timing_metrics = {}
        self.summary_metrics = {}

        self.flush_interval = 10
        self.timer = threading.Timer(self.flush_interval, self.force_flush)
        self.timer.start()

    def force_flush(self):
        self.timer.cancel()

        metrics = []

        sms = self.set_metrics
        self.set_metrics = {}
        for k in sms:
            metrics.append(sms[k])

        cms = self.counter_metrics
        self.counter_metrics = {}
        for k in cms:
            metrics.append(cms[k])

        tms = self.timing_metrics
        self.timing_metrics = {}
        for k in tms:
            metrics.append(tms[k])

        sums = self.summary_metrics
        self.summary_metrics = {}
        for k in sums:
            v = sums[k]
            metrics.append(dict(
                type=v['type'],
                name=v['name'],
                data_type='tdigest',
                value=v['td'].simpleSerialize(),
                output=dict(common=['count', 'min', 'max', 'avg'], percentiles=[5, 95, 99])
            ))

        if not metrics:
            return

        try:
            headers = {'X-App-Token': self.token}
            requests.post(self.send_api, json=dict(metrics=metrics), headers=headers, timeout=2.0)
        except Exception:
            return

    def _flush(self):
        if self.timer.is_alive():
            return
        self.timer = threading.Timer(self.flush_interval, self.force_flush)
        self.timer.start()

    def set(self, name, value, ts=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        last = self.set_metrics.get(key)
        if last:
            last['value'] = value
        else:
            self.set_metrics[key] = dict(type='set', name=name, value=value, ts=ts)
        self._flush()

    def counter(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        last = self.counter_metrics.get(name)
        if last:
            last['value'] += value
        else:
            self.counter_metrics[name] = dict(type='counter', name=name, value=value)
        self._flush()

    def timing(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        last = self.timing_metrics.get(name)
        if last:
            last['count'] += 1
            last['sum'] += value
            last['min'] = min(last['min'], value)
            last['max'] = max(last['max'], value)
        else:
            self.timing_metrics[name] = dict(type='timing', name=name, count=1, sum=value, max=value, min=value)
        self._flush()

    def summary(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        last = self.summary_metrics.get(name)
        if last:
            last['td'].push(value)
        else:
            td = Tdigest()
            td.push(value)
            self.summary_metrics[name] = dict(type='summary', name=name, td=td)
        self._flush()

    def _check_not_empty_string(self, s, label):
        if not isinstance(s, basestring if PY2 else str):  # noqa
            raise MCError(u'%s must be string' % label)
        if not s.strip():
            raise MCError(u'%s cannot be empty' % label)
        return s.strip()

    def _check_number(self, n, label):
        if not isinstance(n, (int, long if PY2 else int, float)):  # noqa
            raise MCError(u'%s must be number' % label)
        return n

    def _check_bool(self, b, label):
        if not isinstance(b, bool):
            raise MCError(u'%s must be bool' % label)
        return b

    def _check_ts(self, ts, label):
        ts_1year = 86400 * 366
        ts = int(self._check_number(ts, label))
        if abs(time.time() - ts) > ts_1year:
            raise MCError(u'value of %s not valid, must between last and next year' % label)
        return ts


class MCError(Exception):
    pass


if __name__ == "__main__":
    print(sys.version)
    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXJzaW9uIjoxLCJhcHBjb2RlIjoib3BzX21ldHJpY2d3IiwidG9fdXNlciI6Inh4eHguemhhbyIsImlhdCI6MTU0NTczNTczMH0.Aj8srWIjyFwxhcMrZlCxyNlP44uLG0iiR31ynyYd4Bw'  # noqa
    send_api = 'http://localhost:6066/v1/metric/send'
    metric = MetricClient(send_api, token)
    metric.set('set', 344)
    metric.summary('summary', 100)
    metric.summary('summary', 200)
    metric.summary('summary', 300)
    metric.summary('summary', 400)

    metric.counter('counter', 100)
    metric.counter('counter', 100)
    metric.timing('timing', 100)
    metric.timing('timing', 200)
    metric.force_flush()

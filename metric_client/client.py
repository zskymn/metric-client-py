# coding=utf-8

import sys
import threading
import multiprocessing
import time
from functools import wraps

import requests
from qtdigest import Tdigest

import logging

try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

PY2 = sys.version_info.major == 2


class MCError(Exception):
    pass


def log_for_error(f):

    @wraps(f)
    def _decorator(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except MCError as ex:
            logging.error('MCError: %s' % ex)
    return _decorator


class MetricClient(object):
    _instance = None
    _instance_lock = threading.Lock()
    _inited = False
    _init_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._instance_lock:
                cls._instance = super(MetricClient, cls).__new__(cls)
        return cls._instance

    def __init__(self, send_api, token):
        with self._init_lock:
            if self._inited:
                return
            self._inited = True
        self.send_api = self._check_not_empty_string(send_api, 'send_api')
        self.token = self._check_not_empty_string(token, 'token')
        self.flush_interval = 10

        self.set_metrics = {}
        self.counter_metrics = {}
        self.max_metrics = {}
        self.min_metrics = {}
        self.avg_metrics = {}
        self.timing_metrics = {}
        self.summary_metrics = {}

        self.set_metrics_lock = threading.Lock()
        self.counter_metrics_lock = threading.Lock()
        self.max_metrics_lock = threading.Lock()
        self.min_metrics_lock = threading.Lock()
        self.avg_metrics_lock = threading.Lock()
        self.timing_metrics_lock = threading.Lock()
        self.summary_metrics_lock = threading.Lock()

        self.timer = None
        self.timer_lock = threading.Lock()

    @log_for_error
    def force_flush(self):
        with self.timer_lock:
            if self.timer:
                self.timer.cancel()

        metrics = []

        with self.set_metrics_lock:
            for k in self.set_metrics:
                metrics.append(self.set_metrics[k])
            self.set_metrics = {}

        with self.counter_metrics_lock:
            for k in self.counter_metrics:
                metrics.append(self.counter_metrics[k])
            self.counter_metrics = {}

        with self.timing_metrics_lock:
            for k in self.timing_metrics:
                metrics.append(self.timing_metrics[k])
            self.timing_metrics = {}

        with self.max_metrics_lock:
            for k in self.max_metrics:
                metrics.append(self.max_metrics[k])
            self.max_metrics = {}

        with self.min_metrics_lock:
            for k in self.min_metrics:
                metrics.append(self.min_metrics[k])
            self.min_metrics = {}

        with self.avg_metrics_lock:
            for k in self.avg_metrics:
                metrics.append(self.avg_metrics[k])
            self.avg_metrics = {}

        with self.summary_metrics_lock:

            for k in self.summary_metrics:
                v = self.summary_metrics[k]
                metrics.append(dict(
                    type=v['type'],
                    name=v['name'],
                    data_type='tdigest',
                    value=v['td'].simpleSerialize(),
                    output=dict(common=['count', 'min', 'max', 'avg'], percentiles=v['percentiles'])
                ))
            self.summary_metrics = {}

        if not metrics:
            return

        headers = {'X-App-Token': self.token}
        try:
            resp = requests.post(self.send_api, json=dict(metrics=metrics), headers=headers)
            if resp.status_code != 200:
                raise MCError(u'gateway api fail, status_code: %s, detai: %s' % (resp.status_code, resp.content))
            data = resp.json()
            if data['errcode'] != 0:
                raise MCError(u'gateway api fail, status_code: 200, detai: %s' % data.get('message'))
        except Exception as ex:
            raise MCError(u'gateway api error: %s' % str(ex))

    def _flush(self):
        with self.timer_lock:
            if self.timer and self.timer.is_alive():
                return
            self.timer = threading.Timer(self.flush_interval, self.force_flush)
            self.timer.start()

    @log_for_error
    def set(self, name, value, ts=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        with self.set_metrics_lock:
            last = self.set_metrics.get(key)
            if last:
                last['value'] = value
            else:
                self.set_metrics[key] = dict(type='set', name=name, value=value, ts=ts)
        self._flush()

    @log_for_error
    def counter(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        with self.counter_metrics_lock:
            last = self.counter_metrics.get(name)
            if last:
                last['value'] += value
            else:
                self.counter_metrics[name] = dict(type='counter', name=name, value=value)
        self._flush()

    @log_for_error
    def max(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        with self.max_metrics_lock:
            last = self.max_metrics.get(name)
            if last:
                last['value'] = max(last['value'], value)
            else:
                self.max_metrics[name] = dict(type='max', name=name, value=value)
        self._flush()

    @log_for_error
    def min(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        with self.min_metrics_lock:
            last = self.min_metrics.get(name)
            if last:
                last['value'] = min(last['value'], value)
            else:
                self.min_metrics[name] = dict(type='min', name=name, value=value)
        self._flush()

    @log_for_error
    def avg(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        with self.avg_metrics_lock:
            last = self.avg_metrics.get(name)
            if last:
                last['count'] += 1
                last['sum'] += value
            else:
                self.avg_metrics[name] = dict(type='avg', name=name, count=1, sum=value)
        self._flush()

    @log_for_error
    def timing(self, name, value):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        with self.timing_metrics_lock:
            last = self.timing_metrics.get(name)
            if last:
                last['count'] += 1
                last['sum'] += value
                last['min'] = min(last['min'], value)
                last['max'] = max(last['max'], value)
            else:
                self.timing_metrics[name] = dict(type='timing', name=name, count=1, sum=value, max=value, min=value)
        self._flush()

    @log_for_error
    def summary(self, name, value, percentiles=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        percentiles = percentiles or [50, 90, 95, 99]
        if not isinstance(percentiles, list):
            raise MCError(u'percentiles must be list')
        for p in percentiles:
            p = self._check_number(p, 'percentile')
            if p < 0 or p > 100:
                raise MCError(u'percentile must between 0 and 100')
        with self.summary_metrics_lock:
            last = self.summary_metrics.get(name)

            if last:
                last['td'].push(value)
                last['percentiles'] = list(set(last['percentiles'] + percentiles))
            else:
                td = Tdigest()
                td.push(value)
                self.summary_metrics[name] = dict(type='summary', name=name, td=td, percentiles=percentiles)
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


if __name__ == "__main__":
    print('python version: %s' % sys.version)

    class TaskWorker(threading.Thread):
        def __init__(self, metric):
            super(TaskWorker, self).__init__()
            self.metric = metric

        def run(self):
            for i in range(100):
                self.metric.summary('summary_metric', i, percentiles=[50, 90, 95, 99])
                self.metric.timing('timing_metric', i * 100)
                self.metric.counter('counter_metric', 1)
                self.metric.max('max_metric', i)
                self.metric.min('min_metric', i)
                self.metric.avg('avg_metric', i)
                self.metric.set('set_metric', i)

    def process_run():
        worker_list = []
        for i in range(50):
            worker_list.append(TaskWorker(metric))

        for worker in worker_list:
            worker.start()

        for worker in worker_list:
            worker.join()

    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXJzaW9uIjoxLCJhcHBjb2RlIjoib3BzX21ldHJpY2d3IiwidG9fdXNlciI6Inh4eHguemhhbyIsImlhdCI6MTU0NTczNTczMH0.Aj8srWIjyFwxhcMrZlCxyNlP44uLG0iiR31ynyYd4Bw'  # noqa
    send_api = 'http://localhost:6066/v1/metric/send'
    metric = MetricClient(send_api, token)
    pool = multiprocessing.Pool(2)

    while True:
        for i in range(20):
            pool.apply_async(process_run)
        time.sleep(1)

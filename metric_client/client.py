# coding=utf-8

import sys
import threading
import multiprocessing
import time
from functools import wraps

import requests
from tdigest import RawTDigest as Tdigest

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
    _instances = {}
    _instance_lock = threading.Lock()
    _initeds = {}
    _init_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._instance_lock:
            ins_id = args + tuple(sorted([(k, v) for k, v in kwargs.items()], key=lambda x: x[0]))
            if not cls._instances.get(ins_id):
                cls._instances[ins_id] = super(MetricClient, cls).__new__(cls)
                cls._instances[ins_id].__ins_id_ = ins_id
            return cls._instances[ins_id]

    def __init__(self, send_api, token):
        with self._init_lock:
            if self._initeds.get(self.__ins_id_):
                return
            self._initeds[self.__ins_id_] = True
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
            self.timer = None

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
                serialize_str = v['td'].simpleSerialize()
                if not serialize_str:
                    continue
                metrics.append(dict(
                    type=v['type'],
                    name=v['name'],
                    data_type='tdigest',
                    value=serialize_str,
                    output=dict(common=['count', 'min', 'max', 'avg'], percentiles=v['percentiles']),
                    ts=v['ts']
                ))
            self.summary_metrics = {}

        self._send_to_gateway(metrics)

    @log_for_error
    def _send_to_gateway(self, metrics):
        def __send(_metrics):
            headers = {'X-App-Token': self.token}
            try:
                resp = requests.post(self.send_api, json=dict(metrics=_metrics), headers=headers)
                return resp
            except Exception:
                try:
                    resp = requests.post(self.send_api, json=dict(metrics=_metrics), headers=headers)
                    return resp
                except Exception as ex:
                    raise MCError(u'gateway api error: %s' % str(ex))

        @log_for_error
        def _send(_metrics):
            resp = __send(_metrics)
            if resp.status_code != 200:
                raise MCError(u'gateway api fail, status_code: %s, detai: %s' % (resp.status_code, resp.content))
            data = resp.json()
            if data['errcode'] != 0:
                raise MCError(u'gateway api fail, status_code: 200, detai: %s' % data.get('message'))

        if not metrics:
            return

        n_limit = 5000
        total = len(metrics)
        for offset in range(0, total, n_limit):
            _send(metrics[offset:offset + n_limit])

    def _flush(self):
        with self.timer_lock:
            if self.timer and self.timer.is_alive():
                return
            self.timer = threading.Timer(self.flush_interval, self.force_flush)
            self.timer.start()

    @log_for_error
    def set(self, name, value, ts=None, agg_labels=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        with self.set_metrics_lock:
            last = self.set_metrics.get(key)
            if last:
                last['value'] = value
            else:
                self.set_metrics[key] = dict(type='set', name=name, value=value, ts=ts, agg_labels=agg_labels)
        self._flush()

    @log_for_error
    def counter(self, name, value, ts=None, agg_labels=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        with self.counter_metrics_lock:
            last = self.counter_metrics.get(key)
            if last:
                last['value'] += value
            else:
                self.counter_metrics[key] = dict(type='counter', name=name, value=value, ts=ts, agg_labels=agg_labels)
        self._flush()

    @log_for_error
    def max(self, name, value, ts=None, agg_labels=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        with self.max_metrics_lock:
            last = self.max_metrics.get(key)
            if last:
                last['value'] = max(last['value'], value)
            else:
                self.max_metrics[key] = dict(type='max', name=name, value=value, ts=ts, agg_labels=agg_labels)
        self._flush()

    @log_for_error
    def min(self, name, value, ts=None, agg_labels=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        with self.min_metrics_lock:
            last = self.min_metrics.get(key)
            if last:
                last['value'] = min(last['value'], value)
            else:
                self.min_metrics[key] = dict(type='min', name=name, value=value, ts=ts, agg_labels=agg_labels)
        self._flush()

    @log_for_error
    def avg(self, name, value, ts=None, agg_labels=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        with self.avg_metrics_lock:
            last = self.avg_metrics.get(key)
            if last:
                last['count'] += 1
                last['sum'] += value
            else:
                self.avg_metrics[key] = dict(type='avg', name=name, count=1, sum=value, ts=ts, agg_labels=agg_labels)
        self._flush()

    @log_for_error
    def timing(self, name, value, ts=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        with self.timing_metrics_lock:
            last = self.timing_metrics.get(key)
            if last:
                last['count'] += 1
                last['sum'] += value
                last['min'] = min(last['min'], value)
                last['max'] = max(last['max'], value)
            else:
                self.timing_metrics[key] = dict(
                    type='timing', name=name, count=1, sum=value, max=value, min=value, ts=ts)
        self._flush()

    @log_for_error
    def summary(self, name, value, percentiles=None, ts=None):
        name = self._check_not_empty_string(name, 'name')
        value = self._check_number(value, 'value')
        ts = self._check_ts(ts or time.time(), 'ts')
        key = '%s::%s' % (name, int(ts / 60))
        percentiles = percentiles or [50, 90, 95, 99]
        if not isinstance(percentiles, list):
            raise MCError(u'percentiles must be list')
        for p in percentiles:
            p = self._check_number(p, 'percentile')
            if p < 0 or p > 100:
                raise MCError(u'percentile must between 0 and 100')
        with self.summary_metrics_lock:
            last = self.summary_metrics.get(key)

            if last:
                last['td'].push(value)
                last['percentiles'] = list(set(last['percentiles'] + percentiles))
            else:
                td = Tdigest()
                td.push(value)
                self.summary_metrics[key] = dict(type='summary', name=name, td=td, percentiles=percentiles, ts=ts)
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
                # self.metric.timing('timing_metric', i * 100, ts=time.time() - 300)
                self.metric.counter('counter_metric', 1, agg_labels=['avg_metric_1', 'avg_metric_2'])
                # self.metric.max('max_metric', i, ts=time.time() - 300)
                # self.metric.min('min_metric', i)
                self.metric.avg('avg_metric', i, agg_labels=['avg_metric_1', 'avg_metric_2'])
                # self.metric.set('set_metric_2', i, agg_labels=['abcdef', 'ab', 'cd'])

    def process_run():
        worker_list = []
        for i in range(5):
            worker_list.append(TaskWorker(metric))

        for worker in worker_list:
            worker.start()

        for worker in worker_list:
            worker.join()

    token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXJzaW9uIjoxLCJhcHBjb2RlIjoib3BzX21ldHJpY2d3IiwidG9fdXNlciI6Inh4eHguemhhbyIsImlhdCI6MTU0NTczNTczMH0.Aj8srWIjyFwxhcMrZlCxyNlP44uLG0iiR31ynyYd4Bw'  # noqa
    send_api = 'http://localhost:6066/v1/metric/send'
    metric = MetricClient(send_api, token)
    pool = multiprocessing.Pool(1)

    while True:
        for i in range(1):
            pool.apply_async(process_run)
        time.sleep(60)

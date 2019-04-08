#! /usr/bin/env python

import collectd
import json
import urllib2
import socket

hostname = socket.gethostname()

CONFIGS = []

def configure_callback(conf):
    """Receive configuration"""

    host = "127.0.0.1"
    port = 5051
    excluded_frameworks = []

    for node in conf.children:
        if node.key == "Host":
            host = node.values[0]
        elif node.key == "Port":
            port = int(node.values[0])
        elif node.key == "ExcludedFrameworks":
            excluded_frameworks = node.values[0].lower().split(",")
        else:
            collectd.warning("mesos-tasks plugin: Unknown config key: %s." % node.key)

    CONFIGS.append({
        "host": host,
        "port": port,
        "excluded_frameworks": excluded_frameworks
    })

def fetch_json(url, timeout=5):
    """Fetch json from url"""
    try:
        return json.load(urllib2.urlopen(url, timeout=timeout))
    except urllib2.URLError, e:
        collectd.error("mesos-tasks plugin: Error connecting to %s - %r" % (url, e))
        return None

def fetch_statistics(conf):
    """Fetch fetch_statistics from slave"""
    return fetch_json("http://%s:%d/monitor/statistics.json" % (conf["host"], conf["port"]), timeout=30)

def fetch_state(conf):
    """Fetch state from slave"""
    return fetch_json("http://%s:%d/state.json" % (conf["host"], conf["port"]))

def read_stats(conf):
    """Read stats from specified slave"""

    statistics = fetch_statistics(conf)
    state = fetch_state(conf)

    if statistics is None or state is None:
        return

    tasks = {}

    for framework in state["frameworks"]:
        if framework["name"].lower() in conf["excluded_frameworks"]:
          continue
        for executor in framework["executors"]:
            for task in executor["tasks"]:
                info = {}

                labels = {}
                if "labels" in task:
                    for label in task["labels"]:
                        labels[label["key"]] = label["value"]

                info["labels"] = labels

                tasks[task["id"]] = info
    # we will aggregate by app name
    ordered_statistics = {}
    for task in statistics:
        app = task["executor_id"].split('.')[0]
        if app not in ordered_statistics:
	    ordered_statistics[app] = [task]
	else:
	    ordered_statistics[app].append(task)

    for app, app_stats in ordered_statistics.iteritems():
        aggregated_metrics = {}
        aggregated_metrics['count'] = len(app_stats) 
    	for task in app_stats:
            if task["source"] not in tasks:
                collectd.warning("mesos-tasks plugin: Task %s found in statistics, but missing in state" % task["source"])
                continue

            info = tasks[task["source"]]
            if "do_not_track" in info["labels"]:
                continue

            for metric, value in task["statistics"].iteritems():
                if metric == 'timestamp':
                    continue
                if metric in aggregated_metrics:
                    aggregated_metrics[metric] += value 
                else:
                    aggregated_metrics[metric] = value
        for metric, value in aggregated_metrics.iteritems():
            read_metric(metric, value, aggregated_metrics['count'], app, None)

def read_metric(metric, value, count, app, metric_prefix=None):
    val = collectd.Values(plugin="mesos-tasks")
    val.type = "gauge"
    val.plugin_instance = "%s-%s" %(app, hostname)
    if metric_prefix is None:
        val.type_instance = metric
    else:
        val.type_instance = "%s-%s" % (metric_prefix, metric )
    if metric == 'count':
        val.values = [value]
    elif isinstance(value, dict):
        for submetric, subvalue in value.iteritems():
            if metric_prefix is None:
                subprefix = metric
            else:
                subprefix = "%s-%s" %(metric_prefix, metric)
            read_metric(submetric, subvalue, count, app, subprefix)
            return
    else:
        val.values = [value/count]
    collectd.info("%s" %val)
    val.dispatch()



def read_callback():
    """Read stats from configured slaves"""
    for conf in CONFIGS:
        read_stats(conf)

collectd.register_config(configure_callback)
collectd.register_read(read_callback)

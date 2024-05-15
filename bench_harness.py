#!/bin/python

import requests
import sys
import time


cluster_config = {
    "proto": "demon",
    "datatype": "counter",
    "nodes": [
        {
            "ip": "localhost",
            "control_port": 5000,
            "db_port": 8080,
            "internal_addr": "bob:1234"
        },
        {
            "ip": "localhost",
            "control_port": 5001,
            "db_port": 8081,
            "internal_addr": "alice:1234"
        },
        {
            "ip": "localhost",
            "control_port": 5002,
            "db_port": 8082,
            "internal_addr": "fabi:1234"
        },
    ]
}

def stop_servers(cluster_config):
    """
    Blocks until all servers that are part of the cluster config are stopped.
    """
    for node in cluster_config["nodes"]:
        requests.post(f"http://{node['ip']}:{node['control_port']}/stop")

def start_servers(cluster_config):
    """
    Starts a cluster with the given config.
    Only returns once all replicas pass the health check.
    """
    root_node = None
    for node in cluster_config["nodes"]:
        config = {
            "proto": cluster_config["proto"],
            "datatype": cluster_config["datatype"],
            "cluster_size": len(cluster_config["nodes"])
        }
        if root_node is None:
            root_node = node
        else:
            config["addr"] = root_node["internal_addr"]
        requests.post(f"http://{node['ip']}:{node['control_port']}/start", json=config)

    time.sleep(1)

    # wait health check on all nodes
    attempts = 0
    while attempts < 5:
        try:
            for node in cluster_config["nodes"]:
                requests.post(f"http://{node['ip']}:{node['db_port']}/")
            break
        except:
            attempts += 1
            time.sleep(attempts)


stop_servers(cluster_config)
start_servers(cluster_config)

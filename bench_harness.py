#!/bin/python

import subprocess
import requests
import sys
import time
import copy
import json

# nodes = {
#     "bob": {
#         "ip": "localhost",
#         "control_port": 5000,
#         "db_port": 8080,
#         "internal_addr": "bob:1234"
#     },
#     "alice": {
#         "ip": "localhost",
#         "control_port": 5001,
#         "db_port": 8081,
#         "internal_addr": "alice:1234"
#     },
#     "fabi": {
#         "ip": "localhost",
#         "control_port": 5002,
#         "db_port": 8082,
#         "internal_addr": "fabi:1234"
#     },
# }

# cluster_config = {
#     "proto": "demon",
#     "datatype": "counter",
#     "nodes_ids": ["bob", "alice", "fabi"]
# }

# bench_config = {
#     "cluster_config": cluster_config,
#     "type": "micro", # tpcc, micro
#     "settings": {
#         "strong_ratio": 0.2,
#         "read_ratio": 0.0,
#         "num_clients": 100,
#         "key_range": 10,
#         "duration": 10,
#     }
# }


def run_benches_from_file(path):
    """
    Reads a json file that specifies the cluster and benchmark configurations.
    Then executes every benchmark configuration.
    """
    with open(path, 'r') as file:
        data = json.load(file)
    nodes = data["nodes"]
    for conf in data["multi_bench_configs"]:
        for bench_config in expand_multi_bench_config(conf):
            run_bench(bench_config, nodes)

def expand_multi_bench_config(multi_config):
    """
    Generates individual benchmark configurations from the more compact json file format.
    """
    for proto in multi_config["cluster_config"]["proto"]:
        for datatype in multi_config["cluster_config"]["datatype"]:
            cluster_config = {
                "proto": proto,
                "datatype": datatype,
                "node_ids": multi_config["cluster_config"]["node_ids"]
            }
            
            if "micro" == multi_config["type"]:
                for settings in expand_micro_bench_settings(multi_config["settings"]):
                    yield {
                        "cluster_config": cluster_config,
                        "type": multi_config["type"],
                        "settings": settings,
                    }
            elif "tpcc" == multi_config["type"]:
                for settings in expand_tpcc_bench_settings(multi_config["settings"]):
                    yield {
                        "cluster_config": cluster_config,
                        "client_nodes": multi_config["client_nodes"],
                        "type": multi_config["type"],
                        "settings": settings,
                    }

def expand_micro_bench_settings(settings):
    """
    Generates settings objects from settings ranges for the micro bench.
    """
    for strong_ratio in settings["strong_ratio"]:
        for read_ratio in settings["read_ratio"]:
            for num_clients in settings["num_clients"]:
                for key_range in settings["key_range"]:
                    for duration in settings["duration"]:
                        yield {
                            "strong_ratio": strong_ratio,
                            "read_ratio": read_ratio,
                            "num_clients": num_clients,
                            "key_range": key_range,
                            "duration": duration,
                        }

def expand_tpcc_bench_settings(settings):
    """
    Generates settings objects from settings ranges for tpcc.
    """
    for num_clients in settings["num_clients"]:
        for scalefactor in settings["scalefactor"]:
            for warehouses in settings["warehouses"]:
                for duration in settings["duration"]:
                    yield {
                        "num_clients": num_clients,
                        "scalefactor": scalefactor,
                        "warehouses": warehouses,
                        "duration": duration,
                    }


previous_tpcc_settings = None
def run_bench(bench_config, nodes):
    """
    Runs a benchmark according to the specified config.

    TODO: collect results
    TODO: tpcc
    """
    global previous_tpcc_settings
    print(f"running: {bench_config}")
    reconfigured = ensure_cluster_state(bench_config["cluster_config"], nodes)
    if "micro" == bench_config["type"]:
        entry_node = nodes[bench_config["cluster_config"]["node_ids"][0]]
        addr = f"{entry_node['ip']}:{entry_node['db_port']}"
        resp = requests.post(f"http://{addr}/bench", json=bench_config["settings"])
        print(resp.json())
    elif "tpcc" == bench_config["type"]:
        settings = bench_config["settings"]
        scalefactor = str(settings["scalefactor"])
        num_clients = str(settings["num_clients"])
        warehouses = str(settings["warehouses"])
        duration = str(settings["duration"])
        command = ["python2", "py-tpcc/pytpcc/coordinator.py", "demon",
                   "--config", "./tpcc_driver.conf",
                   "--scalefactor", scalefactor,
                   "--clientprocs", num_clients,
                   "--warehouses", warehouses,
                   "--duration", duration]
        if not reconfigured:
            if previous_tpcc_settings is not None \
                and previous_tpcc_settings["scalefactor"] == scalefactor \
                and previous_tpcc_settings["warehouses"] == warehouses:
                # we can re-use the data from the previous tpcc run
                command.append("--no-load")
            else:
                # need to re-start the servers to have a clean db
                stop_servers(bench_config["cluster_config"], nodes)
                start_servers(bench_config["cluster_config"], nodes)
        entry_node = nodes[bench_config["cluster_config"]["node_ids"][0]]
        with open("./tpcc_driver.conf", 'w') as file:
            file.write(f"host {entry_node['ip']}\nport {entry_node['db_port']}\nclients: {' '.join(bench_config['client_nodes'])}\npath: /workspace/py-tpcc/pytpcc")
        result = subprocess.run(command, capture_output=True, text=True)
        # TODO: collect results
        print(f"res: {result.stdout}\nerr: {result.stderr}")
    else:
        print("Bad benchmark type. choose one of: micro, tpcc")

current_cluster_config = None
def ensure_cluster_state(cluster_config, nodes):
    """
    Makes sure the cluster is configured according to the arguments.
    Returns `True` if the cluster was reconfigured in the process.
    """
    global current_cluster_config
    old = current_cluster_config
    new = cluster_config
    if old is None \
        or old["proto"] != new["proto"] \
        or old["datatype"] != new["datatype"] \
        or set(old["node_ids"]) != set(new["node_ids"]):
        if old is None:
            stop_servers(new, nodes)
        else:
            stop_servers(old, nodes)
        start_servers(new, nodes)
        current_cluster_config = copy.deepcopy(new)
        return True
    else:
        return False

def stop_servers(cluster_config, nodes):
    """
    Blocks until all servers that are part of the cluster config are stopped.
    """
    for node_id in cluster_config["node_ids"]:
        node = nodes[node_id]
        requests.post(f"http://{node['ip']}:{node['control_port']}/stop")

def start_servers(cluster_config, nodes):
    """
    Starts a cluster with the given config.
    Only returns once all replicas pass the health check.
    """
    root_node = None
    for node_id in cluster_config["node_ids"]:
        node = nodes[node_id]
        config = {
            "proto": cluster_config["proto"],
            "datatype": cluster_config["datatype"],
            "cluster_size": len(cluster_config["node_ids"])
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
            for node_id in cluster_config["node_ids"]:
                node = nodes[node_id]
                requests.post(f"http://{node['ip']}:{node['db_port']}/")
            break
        except:
            attempts += 1
            time.sleep(attempts)
    # just to make sure everything had more than enough time to be fully running
    time.sleep(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("one argument required to specify input file")
        exit()
    path = sys.argv[1]
    run_benches_from_file(path)

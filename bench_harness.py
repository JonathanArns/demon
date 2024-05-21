#!/bin/python

import sys
import time
import copy
import json
import requests
import subprocess
import pandas as pd
from multiprocessing import Pool


def run_benches_from_file(path):
    """
    Reads a json file that specifies the cluster and benchmark configurations.
    Then executes every benchmark configuration.
    """
    with open(path, 'r') as file:
        data = json.load(file)
    nodes = data["nodes"]
    
    results = []

    # local only
    # set_latency(10, nodes)

    for conf in data["multi_bench_configs"]:
        for bench_config in expand_multi_bench_config(conf):
            output = run_bench(bench_config, nodes)
            if output is not None:
                results.append(output)
    
    df = pd.DataFrame(results)
    df.to_csv("experiment_output.csv", index=False)

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
    for scalefactor in settings["scalefactor"]:
        for warehouses in settings["warehouses"]:
            for num_clients in settings["num_clients"]:
                for duration in settings["duration"]:
                    yield {
                        "num_clients": num_clients,
                        "scalefactor": scalefactor,
                        "warehouses": warehouses,
                        "duration": duration,
                    }

def run_micro(args):
    """
    This is a helper function to run the microbench with multiprocessing.
    """
    measurements = requests.post(f"http://{args[1]}/bench", json=args[2]).json()
    measurements["node_id"] = args[0]
    return measurements

previous_tpcc_settings = None
def run_bench(bench_config, nodes):
    """
    Runs a benchmark according to the specified config.

    Returns a flat dict containing measurements and parameters, to be inserted as a row into a DataFrame.
    """
    global previous_tpcc_settings
    print(f"running: {bench_config}")
    reconfigured = ensure_cluster_state(bench_config["cluster_config"], nodes)
    time.sleep(1)
    if "micro" == bench_config["type"]:
        args = [(id, f"{nodes[id]['ip']}:{nodes[id]['db_port']}", bench_config["settings"]) for id in bench_config["cluster_config"]["node_ids"]]
        with Pool(processes=len(args)) as pool:
            results = pool.map(run_micro, args)

        # record benchmark measurements
        data = {
            "total_mean_latency": 0.0,
            "total_throughput": 0,
        }
        for values in results:
            data["total_throughput"] += values["throughput"]
            data["total_mean_latency"] += values["mean_latency"]
            data[f"{values['node_id']}_throughput"] = values["throughput"]
            data[f"{values['node_id']}_mean_latency"] = values["mean_latency"]
        data["total_mean_latency"] /= len(results)

        # record benchmark parameters
        data["datatype"] = bench_config["cluster_config"]["datatype"]
        data["proto"] = bench_config["cluster_config"]["proto"]
        data["cluster_size"] = len(args)
        data["strong_ratio"] = bench_config["settings"]["strong_ratio"]
        data["read_ratio"] = bench_config["settings"]["read_ratio"]
        data["read_ratio"] = bench_config["settings"]["read_ratio"]
        data["duration"] = bench_config["settings"]["duration"]
        data["num_clients"] = bench_config["settings"]["num_clients"]
        data["key_range"] = bench_config["settings"]["key_range"]

        return data

    elif "tpcc" == bench_config["type"]:
        settings = bench_config["settings"]
        scalefactor = str(settings["scalefactor"])
        num_clients = str(settings["num_clients"])
        warehouses = str(settings["warehouses"])
        duration = str(settings["duration"])
        command = ["python", "py-tpcc/pytpcc/coordinator.py", "demon",
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
        with open("./tpcc_driver.conf", 'w') as file:
            # we assume that the clients are co-located with a db node that they can reach at localhost:80
            file.write(f"[demon]\nhost: localhost\nport: 80\nclients: {','.join(bench_config['client_nodes'])}\npath: /workspace/py-tpcc/pytpcc")
        result = subprocess.run(command, capture_output=True, text=True)

        # TODO: collect results
        print(f"res: {result.stdout}\nerr: {result.stderr}")
        return None

    else:
        print("Bad benchmark type. choose one of: micro, tpcc")
        return None

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

def set_latency(ms, nodes):
    for node in nodes.values():
        body = { "cmd": f"tc qdisc add dev eth0 root netem delay {ms}ms" }
        requests.post(f"http://{node['ip']}:{node['control_port']}/stop")
        requests.post(f"http://{node['ip']}:{node['control_port']}/run_cmd", json=body)
    time.sleep(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("one argument required to specify input file")
        exit()
    path = sys.argv[1]
    run_benches_from_file(path)

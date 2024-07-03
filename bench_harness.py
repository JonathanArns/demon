#!/bin/python

import os
import sys
import time
import copy
import json
import requests
import subprocess
import pandas as pd
from multiprocessing import Pool


def run_bench_loop(path):
    counter = 1
    while True:
        run_benches_from_file(path, write_output=False, silent=True, latencies=False)
        print(f"ran {counter} times")
        counter += 1

def run_benches_from_file(path, write_output=True, output_dir="./test_data/", silent=False, latencies=True, forget_protos=[], always_load=False):
    """
    Reads a json file that specifies the cluster and benchmark configurations.
    Then executes every benchmark configuration.
    """
    try:
        os.mkdir(output_dir)
    except Exception as e:
        pass
    with open(path, 'r') as file:
        data = json.load(file)
    nodes = data["nodes"]
    
    # try to load previous bench state
    try:
        with open(os.path.join(output_dir, "bench_state.json"), "r") as file:
            bench_state = json.load(file)
    except Exception:
        bench_state = {
            "micro_results": [],
            "tpcc_results": [],
            "finished_benches": [],
            "rtt_latencies": measure_rtt_latency(nodes) if latencies else None,
        }

    # forget results from protocols that should be run again
    if len(forget_protos) > 0:
        if not silent:
            print(f"forgetting state and results for protocols {forget_protos}")
        bench_state["micro_results"] = [item for item in bench_state["micro_results"] if not item["proto"] in forget_protos]
        bench_state["tpcc_results"] = [item for item in bench_state["tpcc_results"] if not item["proto"] in forget_protos]
        bench_state["finished_benches"] = [item for item in bench_state["finished_benches"] if not json.loads(item)["cluster_config"]["proto"] in forget_protos]

    # enumerate exeperiments and execute them
    for conf in data["multi_bench_configs"]:
        for bench_config in expand_multi_bench_config(conf):
            repr = json.dumps(bench_config, sort_keys=True)
            if repr in bench_state["finished_benches"]:
                continue
            output = run_bench(bench_config, nodes, silent, always_load)
            bench_state["finished_benches"].append(repr)
            if output is not None:
                if "tpcc" == bench_config["type"]:
                    bench_state["tpcc_results"].append(output)
                elif "micro" == bench_config["type"]:
                    bench_state["micro_results"].append(output)
            if write_output:
                with open(os.path.join(output_dir, "bench_state.json"), "w") as file:
                    json.dump(bench_state, file)
    
    # write final results as csv files
    tpcc_df = pd.DataFrame(bench_state["tpcc_results"])
    micro_df = pd.DataFrame(bench_state["micro_results"])
    if write_output:
        if len(bench_state["tpcc_results"]) > 0:
            tpcc_df.to_csv(os.path.join(output_dir, "tpcc.csv"), index=False)
        if len(bench_state["micro_results"]) > 0:
            micro_df.to_csv(os.path.join(output_dir, "micro_bench.csv"), index=False)
        with open(os.path.join(output_dir, "rtt_latencies.json"), "w") as f:
            json.dump(bench_state["rtt_latencies"], f, indent=4)
    elif not silent:
        if len(tpcc_df.columns) > 0:
            print(tpcc_df[["proto", "total_throughput", "total_mean_latency", "total_count"]])
        if len(micro_df.columns) > 0:
            print(micro_df[["proto", "total_throughput", "total_mean_latency"]])

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
    try:
        measurements = requests.post(f"http://{args[1]}/bench", json=args[2], timeout=args[3]).json()
        measurements["node_id"] = args[0]
        return measurements
    except Exception:
        return None

def run_tpcc(args):
    """
    This is a helper function to run the tpcc bench with multiprocessing.
    """
    try:
        output = requests.post(f"http://{args[1]}/run_cmd", json=args[2], timeout=args[3]).json()
        output["node_id"] = args[0]
        return output
    except Exception as e:
        print(e)
        return None

previous_tpcc_settings = None
def run_bench(bench_config, nodes, silent=False, always_load=False):
    """
    Runs a benchmark according to the specified config.

    Returns a flat dict containing measurements and parameters, to be inserted as a row into a DataFrame.
    """
    global previous_tpcc_settings
    if not silent:
        print(f"running: {bench_config}")
    reconfigured = ensure_cluster_state(bench_config["cluster_config"], nodes, always_load)
    while True:
        try:
            time.sleep(1)
            if "micro" == bench_config["type"]:
                args = [(id, f"{nodes[id]['ip']}:{nodes[id]['db_port']}", bench_config["settings"], 2 * bench_config["settings"]["duration"]) for id in bench_config["cluster_config"]["node_ids"]]
                with Pool(processes=len(args)) as pool:
                    results = pool.map(run_micro, args)
                if None in results:
                    raise Exception("micro results missing from at least one process")

                # record benchmark measurements
                data = {
                    "total_mean_latency": 0.0,
                    "total_p95_latency": 0.0,
                    "total_p99_latency": 0.0,
                    "total_throughput": 0,
                }
                for values in results:
                    data["total_throughput"] += values["totals"]["throughput"]
                    data["total_mean_latency"] += values["totals"]["mean_latency"]
                    data["total_p95_latency"] += values["totals"]["p95_latency"]
                    data["total_p99_latency"] += values["totals"]["p99_latency"]
                    data[f"{values['node_id']}_throughput"] = values["totals"]["throughput"]
                    data[f"{values['node_id']}_mean_latency"] = values["totals"]["mean_latency"]
                    data[f"{values['node_id']}_p95_latency"] = values["totals"]["p95_latency"]
                    data[f"{values['node_id']}_p99_latency"] = values["totals"]["p99_latency"]
                    for op in values["per_op"].keys():
                        if not f"{op}_mean_latency" in data:
                            data[f"{op}_mean_latency"] = 0.0
                            data[f"{op}_p95_latency"] = 0.0
                            data[f"{op}_p99_latency"] = 0.0
                            data[f"{op}_throughput"] = 0
                        data[f"{op}_mean_latency"] += values["per_op"][op]["mean_latency"]
                        data[f"{op}_p95_latency"] += values["per_op"][op]["p95_latency"]
                        data[f"{op}_p99_latency"] += values["per_op"][op]["p99_latency"]
                        data[f"{op}_throughput"] += values["per_op"][op]["throughput"]
                        
                data["total_mean_latency"] /= len(results)

                # record benchmark parameters
                data["datatype"] = bench_config["cluster_config"]["datatype"]
                data["proto"] = bench_config["cluster_config"]["proto"]
                data["cluster_size"] = len(bench_config["cluster_config"]["node_ids"])
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
                command = ["python", "/workspace/py-tpcc/pytpcc/tpcc.py", "demon",
                        "--config", "/workspace/demon_driver.conf",
                        "--scalefactor", scalefactor,
                        "--warehouses", warehouses,
                        "--duration", duration]

                load = True
                if not reconfigured:
                    if not always_load \
                        and previous_tpcc_settings is not None \
                        and str(previous_tpcc_settings["scalefactor"]) == scalefactor \
                        and str(previous_tpcc_settings["warehouses"]) == warehouses:
                        # we can re-use the data from the previous tpcc run
                        load = False
                    else:
                        # need to re-start the servers to have a clean db
                        stop_servers(bench_config["cluster_config"], nodes)
                        start_servers(bench_config["cluster_config"], nodes)
                previous_tpcc_settings = copy.deepcopy(settings)

                if load:
                    if not silent:
                        print("loading tpcc data")
                    node = nodes[bench_config["cluster_config"]["node_ids"][0]]
                    output = requests.post(f"http://{node['ip']}:{node['control_port']}/run_cmd", json={"cmd": " ".join(command + ["--no-execute", "--clients", "4"])}).json()
                    if not silent and len(output["std_err"]) > 0:
                        print(f"STDOUT:\n{output['std_out']}\nSTDERR:\n{output['std_err']}")
                    if "Failed to load" in output["std_err"]:
                        raise Exception("failed to load TPCC data")

                    time.sleep(1)
                else:
                    if not silent:
                        print("skipped loading tpcc data")

                # now execute in threadpool
                print("executing tpcc bench")
                args = [(id, f"{nodes[id]['ip']}:{nodes[id]['control_port']}", {"cmd": " ".join(command + ["--no-load", "--clients", num_clients])}, 2 * settings["duration"]) for id in bench_config["cluster_config"]["node_ids"]]
                with Pool(processes=len(args)) as pool:
                    results = pool.map(run_tpcc, args)
                if None in results:
                    raise Exception("tpcc results missing from at least one process")

                data = {}
                data["datatype"] = bench_config["cluster_config"]["datatype"]
                data["proto"] = bench_config["cluster_config"]["proto"]
                data["cluster_size"] = len(bench_config["cluster_config"]["node_ids"])
                data["duration"] = bench_config["settings"]["duration"]
                data["num_clients"] = bench_config["settings"]["num_clients"]
                data["total_throughput"] = 0
                data["total_mean_latency"] = 0.0
                data["total_count"] = 0
                data["total_time"] = 0.0
                for result in results:
                    try:
                        node_id = result["node_id"]
                        replica_data = json.loads(result['std_out'])
                        data["total_throughput"] += replica_data["total_throughput"]
                        data["total_mean_latency"] += replica_data["total_mean_latency"]
                        data["total_count"] += replica_data["total_count"]
                        data["total_time"] += replica_data["total_time"]
                        for key, val in replica_data.items():
                            data[f"{node_id}_{key}"] = val
                    except Exception as e:
                        raise Exception(f"couldn't load TPCC results with exception {e}\nSTDOUT:\n{result['std_out']}\nSTDERR:\n{result['std_err']}")
                data["total_mean_latency"] /= len(results)
                return data

            else:
                print("Bad benchmark type. choose one of: micro, tpcc")
                return None
        except Exception as e:
            print(f"retrying bench after exception: {e}")
            stop_servers(bench_config["cluster_config"], nodes)
            time.sleep(5)
            start_servers(bench_config["cluster_config"], nodes)
            reconfigured = True

current_cluster_config = None
def ensure_cluster_state(cluster_config, nodes, force_restart=False):
    """
    Makes sure the cluster is configured according to the arguments.
    Returns `True` if the cluster was reconfigured in the process.
    """
    global current_cluster_config
    old = current_cluster_config
    new = cluster_config
    if force_restart \
        or old is None \
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

        attempts = 0
        while attempts < 5:
            try:
                requests.post(f"http://{node['ip']}:{node['control_port']}/stop", timeout=3)
                break
            except:
                attempts += 1
                time.sleep(attempts)
        assert attempts < 5, "could not stop node"

def start_servers(cluster_config, nodes):
    """
    Starts a cluster with the given config.
    Only returns once all replicas pass the health check.
    """
    root_addr = None
    for node_id in cluster_config["node_ids"]:
        node = nodes[node_id]
        config = {
            "proto": cluster_config["proto"],
            "datatype": cluster_config["datatype"],
            "cluster_size": len(cluster_config["node_ids"]),
            "name": str(node_id),
        }
        if root_addr is None:
            if node["internal_ip"] is None:
                root_addr = f"{node['ip']}:{node['internal_port']}"
            else:
                root_addr = f"{node['internal_ip']}:{node['internal_port']}"
        else:
            config["addr"] = root_addr
        requests.post(f"http://{node['ip']}:{node['control_port']}/start", json=config, timeout=3)

    time.sleep(1)

    # wait health check on all nodes
    attempts = 0
    while attempts < 5:
        try:
            for node_id in cluster_config["node_ids"]:
                node = nodes[node_id]
                requests.post(f"http://{node['ip']}:{node['db_port']}/", timeout=3)
            break
        except:
            attempts += 1
            time.sleep(attempts)
    assert attempts < 5, "could not start cluster"
    # just to make sure everything had more than enough time to be fully running
    time.sleep(1)

def measure_rtt_latency(nodes):
    # start a cluster with all nodes
    ensure_cluster_state({
        "proto": "demon",
        "datatype": "non-neg-counter",
        "node_ids": [id for id in nodes.keys()],
    }, nodes)

    latencies = {}
    for id, node in nodes.items():
        resp = requests.get(f"http://{node['ip']}:{node['db_port']}/measure_rtt_latency", timeout=10)
        measurements = resp.json()
        latencies[id] = {}
        for item in measurements:
            latencies[id][item[1]] = item[2]
    return latencies

def set_latency(ms, nodes):
    for node in nodes.values():
        body = { "cmd": f"tc qdisc add dev eth0 root netem delay {ms}ms" }
        requests.post(f"http://{node['ip']}:{node['control_port']}/stop")
        requests.post(f"http://{node['ip']}:{node['control_port']}/run_cmd", json=body)
    time.sleep(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("argument required to specify input file")
        exit()

    args = sys.argv[1:]
    write_output = True
    output_dir = "./experiment_output"
    forget = []
    loop = False
    always_load = True
    if "--no-write" in args:
        args.remove("--no-write")
        write_output = False
    if "--output-dir" in args:
        idx = args.index("--output-file")
        args.pop(idx)
        output_dir = args.pop(idx)
    if "-o" in args:
        idx = args.index("-o")
        args.pop(idx)
        output_dir = args.pop(idx)
    while "--forget-proto" in args:
        idx = args.index("--forget-proto")
        args.pop(idx)
        forget.append(args.pop(idx))
    while "-f" in args:
        idx = args.index("-f")
        args.pop(idx)
        forget.append(args.pop(idx))
    if "--loop" in args:
        args.remove("--loop")
        loop = True
    if "--reuse" in args:
        args.remove("--reuse")
        always_load = False

    if len(args) < 1:
        print("argument required to specify input file")
        exit()
    path = args[0]
    
    if loop:
        run_bench_loop(path)
    else:
        run_benches_from_file(path, write_output=write_output, output_dir=output_dir, forget_protos=forget, always_load=always_load)

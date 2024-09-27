import os
import sys
import matplotlib.pyplot as plt
import dask.dataframe as dd
import pandas as pd
import numpy as np
import scipy.stats as st
import json


PLOT_STYLES = {
    "demon": "o-",
    "gemini": "o-",
    "redblue": "o-",
    "unistore": "o-",
    "causal": "o-",
    "strict": "o-",
}

COLORS = {
    "demon": "tab:blue",
    "gemini": "tab:red",
    "redblue": "tab:green",
    "unistore": "tab:orange",
    "causal": "tab:purple",
    "strict": "tab:brown",
}

LABELS = {
    "demon": "DeMon",
    "gemini": "RedBlue (gemini)",
    "redblue": "RedBlue (ft)",
    "unistore": "UniStore",
    "causal": "Causal",
    "strict": "Strict",
}


def plot_rubis(df, dir_path):
    df = df[df["datatype"] == "rubis"]
    if len(df) == 0:
        return

    for cluster_size in df["cluster_size"].unique():
        cluster_df = df[df["cluster_size"] == cluster_size]
        protocols = df["proto"].unique()
        operations = ["GetAuction", "GetItem", "OpenAuction", "CloseAuction", "Bid", "Sell", "BuyNow"] 
        stats = {proto: [] for proto in protocols}
        for proto in protocols:
            proto_df = cluster_df[cluster_df["proto"] == proto]
            for operation in operations:
                op_df = proto_df[proto_df["op"] == operation]
                stats[proto].append(calc_stats(op_df["latency_micros"]))

        plt.figure(figsize=(6, 6))  # Adjust size as needed
        plt.suptitle(f"Rubis-like benchmark with {cluster_size} replicas")
        
        index = np.arange(len(operations))
        bar_width = 0.1

        i = 0
        for proto, stats in stats.items():
            mean = [item["mean"] for item in stats]
            mean_conf = [item["mean_conf_int"] for item in stats]
            # p99 = [item["p99"] for item in stats]
            plt.bar(
                x=index + 1.5*i*bar_width,
                height=[item["mean"] for item in stats],
                yerr=([item["mean"] - item["mean_conf_int"].low for item in stats], [item["mean_conf_int"].high - item["mean"] for item in stats]),
                width=bar_width,
                label=proto,
                capsize=1.0
            )
            i += 1
        plt.xticks(index + 0.5 * 1.5 * bar_width * (len(protocols)-1), operations)
        plt.ylabel("mean latency (ms)")
        # plt.title(f"mean latency per operation")
        plt.legend()
        plt.savefig(os.path.join(dir_path, f"rubis_bar_plot_{cluster_size}_nodes.png"), dpi=300)

        # plt.subplot(2, 1, 2)
        # i = 0
        # for operation, vals in p99_latencies.items():
        #     plt.bar(index + 1.5*i*bar_width, vals, bar_width, label=operation)
        #     i += 1
        # plt.xticks(index + 0.5 * 1.5 * bar_width * (len(protocols)-1), operations)
        # plt.ylabel("p99 latency (ms)")
        # # plt.title(f"tail latency per operation")
        # plt.legend()

        plt.savefig(os.path.join(dir_path, f"rubis_bar_plot_{cluster_size}_nodes.png"), dpi=300)
    


def aggregate(benches, dir_path, recompute=True):
    if recompute:
        aggregate = []
        for filename in benches:
            df = pd.read_csv(os.path.join(dir_path, filename))
            df["op"] = df["meta"].str.split(" ").str.get(1)
            init = df[df["kind"] == "initiated"]
            client = init.merge(df[df["kind"] == "visible"][["meta", "node", "unix_micros"]], on=["meta", "node"], suffixes=("", "_client_visible"))
            client["client_latency"] = (client["unix_micros_client_visible"] - client["unix_micros"]) / 1000

            last_visible = df[df["kind"] == "visible"].sort_values(by=["unix_micros"]).drop_duplicates(subset=["meta"], keep="last")
            remote = init.merge(last_visible[["meta", "unix_micros"]], on=["meta"], suffixes=("", "_all_visible"))
            remote["remote_latency"] = (remote["unix_micros_all_visible"] - remote["unix_micros"]) / 1000
            agg = {
                "datatype": df["datatype"].iloc[0],
                "proto": df["proto"].iloc[0],
                "num_clients": df["num_clients"].iloc[0],
                "cluster_size": df["cluster_size"].iloc[0],
                "strong_ratio": df["strong_ratio"].iloc[0],
                "key_range": df["key_range"].iloc[0],
                "duration": df["duration"].iloc[0],
                "remote_mean_latency": remote["remote_latency"].mean(),
                "remote_p95_latency": remote["remote_latency"].quantile(0.95),
                "remote_p99_latency": remote["remote_latency"].quantile(0.99),
                "client_mean_latency": client["client_latency"].mean(),
                "client_p95_latency": client["client_latency"].quantile(0.95),
                "client_p99_latency": client["client_latency"].quantile(0.99),
                "throughput": remote["kind"].count() / df["duration"].iloc[0],
            }
            for op in init["op"].unique():
                op_client = client[client["op"] == op]
                op_remote = remote[remote["op"] == op]
                agg[f"{op}_remote_mean_latency"] = op_remote["remote_latency"].mean()
                agg[f"{op}_remote_p95_latency"] = op_remote["remote_latency"].quantile(0.95)
                agg[f"{op}_remote_p99_latency"] = op_remote["remote_latency"].quantile(0.99)
                agg[f"{op}_client_mean_latency"] = op_client["client_latency"].mean()
                agg[f"{op}_client_p95_latency"] = op_client["client_latency"].quantile(0.95)
                agg[f"{op}_client_p99_latency"] = op_client["client_latency"].quantile(0.99)
            aggregate.append(agg)
            del df, init, last_visible, client, remote
        df = pd.DataFrame(aggregate)
        df.sort_values(by=["datatype", "cluster_size", "proto", "num_clients"], inplace=True)
        df.to_csv(os.path.join(dir_path, "aggregate.csv"), index=False)
    else:
        df = pd.read_csv(os.path.join(dir_path, "aggregate.csv"))
    return df



def plot_non_neg_counter_op_latency(df, dir_path):
    df = df[df["datatype"] == "non-neg-counter"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    df = df[df["num_clients"] == 1000]
    df = df[df["strong_ratio"] == 0.5]
    protocols = df["proto"].unique()
    ops = ["Add", "Subtract"]
    index = np.arange(len(ops))
    bar_width = 0.15
    plt.figure(figsize=(6, 6))  # Adjust size as needed
    i = 0
    j = 0
    for proto in protocols:
        proto_df = df[df["proto"] == proto]
        plt.bar(
            x=index + i * bar_width,
            height=[proto_df[f"{op}_client_mean_latency"].iloc[0] for op in ops],
            yerr=([0 for x in range(len(ops))], [proto_df[f"{op}_client_p95_latency"].iloc[0] for op in ops]),
            width=bar_width,
            label=LABELS[proto],
            capsize=3.0,
            color=COLORS[proto],
        )
        i += 1
    plt.xticks([x + 0.5 * bar_width * (len(protocols)-1) for x in range(len(ops))], ops)
    plt.ylabel("mean client latency (ms)")
    plt.legend()
    plt.savefig(os.path.join(dir_path, f"non-neg-counter-client-latency-bars.png"), dpi=300)

    plt.figure(figsize=(6, 6))  # Adjust size as needed
    i = 0
    j = 0
    for proto in protocols:
        proto_df = df[df["proto"] == proto]
        plt.bar(
            x=index + i * bar_width,
            height=[proto_df[f"{op}_remote_mean_latency"].iloc[0] for op in ops],
            yerr=([0 for x in range(len(ops))], [proto_df[f"{op}_remote_p95_latency"].iloc[0] for op in ops]),
            width=bar_width,
            label=LABELS[proto],
            capsize=3.0,
            color=COLORS[proto],
        )
        i += 1
    plt.xticks([x + 0.5 * bar_width * (len(protocols)-1) for x in range(len(ops))], ops)
    plt.ylabel("mean remote latency (ms)")
    plt.legend()
    plt.savefig(os.path.join(dir_path, f"non-neg-counter-remote-latency-bars.png"), dpi=300)

def plot_rubis_throughput(df, dir_path):
    df = df[df["datatype"] == "rubis"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    protocols = df["proto"].unique()
    plt.figure(figsize=(10, 6))  # Adjust size as needed
    for proto in protocols:
        proto_df = df[df["proto"] == proto]
        # plt.errorbar(proto_df["throughput"], proto_df["remote_mean_latency"], yerr=([0 for x in proto_df["proto"]], proto_df["remote_p95_latency"]), fmt=PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
        plt.plot(proto_df["throughput"], proto_df["remote_mean_latency"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
    plt.xlabel("throughput (txns/s)")
    plt.ylabel("mean remote latency (ms)")
    plt.title(f"RUBiS remote latency by throughput")
    plt.legend()
    plt.savefig(os.path.join(dir_path, f"rubis_throughput.png"), dpi=300)



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("argument required to specify input directory")
        exit()

    args = sys.argv[1:]
    recompute = False
    if "--recompute" in args:
        args.remove("--recompute")
        recompute=True
    path = args[0]
    figure = args[1]

    with open(os.path.join(path, "bench_state.json"), "r") as file:
        bench_state = json.load(file)
    benches = bench_state["finished_benches"]

    df = aggregate(benches, path, recompute)
    if figure == "rubis-throughput":
        plot_rubis_throughput(df, path)
    elif figure == "non-neg-latency-bars":
        plot_non_neg_counter_op_latency(df, path)
    else:
        print("not a known figure name")

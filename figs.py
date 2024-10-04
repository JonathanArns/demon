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
    "gemini": "v-",
    "redblue": "^-",
    "unistore": ">-",
    "causal": "<-",
    "strict": "+-",
}
P99_PLOT_STYLES = {
    "demon": "o:",
    "gemini": "v:",
    "redblue": "^:",
    "unistore": ">:",
    "causal": "<:",
    "strict": "+:",
}

BAR_PATTERNS = {
    "demon": "+",
    "gemini": "/",
    "redblue": "\\",
    "unistore": "x",
    "causal": "|",
    "strict": "-",
}

COLORS = {
    "demon": "tab:blue",
    "gemini": "tab:red",
    "redblue": "tab:purple",
    "unistore": "tab:green",
    "causal": "tab:orange",
    "strict": "tab:brown",
}

LABELS = {
    "demon": "DeMon",
    "gemini": "RedBlue (gemini)",
    "redblue": "RedBlue (ft)",
    "unistore": "UniStore",
    "causal": "No guarantees",
    "strict": "Strict",
}

PROTOS = ["demon", "gemini", "redblue", "unistore", "strict", "causal"]


def aggregate(dir_path, recompute=True):
    if recompute:
        with open(os.path.join(dir_path, "bench_state.json"), "r") as file:
            bench_state = json.load(file)
        benches = bench_state["finished_benches"]

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
                "remote_median_latency": remote["remote_latency"].median(),
                "remote_p95_latency": remote["remote_latency"].quantile(0.95),
                "remote_p99_latency": remote["remote_latency"].quantile(0.99),
                "client_mean_latency": client["client_latency"].mean(),
                "client_median_latency": client["client_latency"].median(),
                "client_p95_latency": client["client_latency"].quantile(0.95),
                "client_p99_latency": client["client_latency"].quantile(0.99),
                "throughput": remote["kind"].count() / df["duration"].iloc[0],
            }
            if agg["proto"] == "demon" and "val" in df.columns:
                agg["mean_unstable_ops_count"] = df[df["kind"] == "num_unstable_ops"]["val"].mean()
            for op in init["op"].unique():
                op_client = client[client["op"] == op]
                op_remote = remote[remote["op"] == op]
                agg[f"{op}_remote_mean_latency"] = op_remote["remote_latency"].mean()
                agg[f"{op}_remote_median_latency"] = op_remote["remote_latency"].median()
                agg[f"{op}_remote_p95_latency"] = op_remote["remote_latency"].quantile(0.95)
                agg[f"{op}_remote_p99_latency"] = op_remote["remote_latency"].quantile(0.99)
                agg[f"{op}_client_mean_latency"] = op_client["client_latency"].mean()
                agg[f"{op}_client_median_latency"] = op_client["client_latency"].median()
                agg[f"{op}_client_p95_latency"] = op_client["client_latency"].quantile(0.95)
                agg[f"{op}_client_p99_latency"] = op_client["client_latency"].quantile(0.99)
            aggregate.append(agg)
            del df, init, last_visible, client, remote
        df = pd.DataFrame(aggregate)
        df.sort_values(by=["datatype", "cluster_size", "proto", "num_clients", "strong_ratio"], inplace=True)
        df.to_csv(os.path.join(dir_path, "aggregate.csv"), index=False)
    else:
        df = pd.read_csv(os.path.join(dir_path, "aggregate.csv"))
    return df


def plot_latency_bars(df, dir_path, datatype="", duration=60, cluster_size=5, num_clients=1000, strong_ratio=0.5, ops=[], limit=None):
    df = df[df["datatype"] == datatype]
    df = df[df["duration"] == duration]
    df = df[df["cluster_size"] == cluster_size]
    df = df[df["num_clients"] == num_clients]
    df = df[df["strong_ratio"] == strong_ratio]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]
    index = np.arange(len(ops))
    bar_width = 1
    for kind in ["remote", "client"]:
        plt.figure(figsize=(3 * len(ops), 4))
        if limit:
            plt.ylim(None, limit)
        i = 0
        j = 0
        for proto in protocols:
            proto_df = df[df["proto"] == proto]
            plt.bar(
                x=index * (len(protocols) + 1) * bar_width + i * bar_width,
                height=[proto_df[f"{op}_{kind}_mean_latency"].iloc[0] for op in ops],
                yerr=([0 for x in range(len(ops))], [proto_df[f"{op}_{kind}_p99_latency"].iloc[0] for op in ops]),
                width=bar_width,
                label=LABELS[proto],
                capsize=5.0,
                color=COLORS[proto],
                edgecolor="black",
                hatch=BAR_PATTERNS[proto],
            )
            i += 1
        plt.xticks([(x+0.5) * bar_width * (len(protocols) + 1) - 1 for x in range(len(ops))], ops)
        plt.ylabel(f"mean {kind} latency (ms)")
        plt.legend(fontsize="large")
        plt.savefig(os.path.join(dir_path, f"{datatype}-{kind}-latency-bars.png"), dpi=300)

def plot_scaling(df, dir_path, datatype="", duration=60, num_clients=2000, limit=None):
    df = df[df["proto"] == "demon"]
    df = df[df["datatype"] == datatype]
    df = df[df["duration"] == duration]
    df = df[df["num_clients"] == num_clients]
    # df = df[(df["num_clients"] == 3333) | (df["num_clients"] == 1429) | ((df["num_clients"] == 2000) & (df["cluster_size"] == 5))]
    cluster_sizes = [3, 5, 7]
    index = np.arange(len(cluster_sizes))
    bar_width = 1
    for kind in ["remote", "client"]:
        plt.figure(figsize=(2 * len(cluster_sizes), 6))
        if limit:
            plt.ylim(None, limit)
        plt.bar(
            x=index * bar_width,
            height=[df[df["cluster_size"] == size][f"{kind}_mean_latency"].iloc[0] for size in cluster_sizes],
            yerr=([0 for x in range(len(cluster_sizes))], [df[df["cluster_size"] == size][f"{kind}_p99_latency"].iloc[0] for size in cluster_sizes]),
            width=bar_width,
            label=LABELS["demon"],
            capsize=5.0,
            color=COLORS["demon"],
            edgecolor="black",
            hatch=BAR_PATTERNS["demon"],
        )
        plt.xticks([x for x in range(len(cluster_sizes))], cluster_sizes)
        plt.xlabel("number of regions")
        plt.ylabel(f"mean {kind} latency (ms)")
        plt.savefig(os.path.join(dir_path, f"{kind}-latency-scaling.png"), dpi=300)

    plt.figure(figsize=(2 * len(cluster_sizes), 6))
    if limit:
        plt.ylim(None, limit)
    plt.bar(
        x=index * bar_width,
        height=[df[df["cluster_size"] == size][f"throughput"].iloc[0] for size in cluster_sizes],
        width=bar_width,
        label=LABELS["demon"],
        capsize=5.0,
        color=COLORS["demon"],
        edgecolor="black",
        hatch=BAR_PATTERNS["demon"],
    )
    plt.xticks([x for x in range(len(cluster_sizes))], cluster_sizes)
    plt.xlabel("number of regions")
    plt.ylabel(f"throughput (txn/s)")
    plt.savefig(os.path.join(dir_path, f"throughput-scaling.png"), dpi=300)



def plot_rubis_lines(df, dir_path):
    df = df[df["datatype"] == "rubis"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]

    for kind in ["client", "remote"]:
        plt.figure(figsize=(7, 4))  # Adjust size as needed
        plt.ylim(None, 1500)
        for proto in protocols:
            proto_df = df[df["proto"] == proto]
            plt.plot(proto_df["num_clients"], proto_df[f"{kind}_mean_latency"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
            plt.plot(proto_df["num_clients"], proto_df[f"{kind}_p99_latency"], P99_PLOT_STYLES[proto], label=LABELS[proto] + " p99", color=COLORS[proto])
        plt.xlabel("clients per region")
        plt.ylabel(f"mean {kind} latency (ms)")
        plt.legend(bbox_to_anchor=(0.5, 1.32), loc="upper center", ncol=3)
        plt.savefig(os.path.join(dir_path, f"rubis_{kind}_latency.png"), dpi=300)

    plt.figure(figsize=(7, 4))  # Adjust size as needed
    for proto in protocols:
        proto_df = df[df["proto"] == proto]
        plt.plot(proto_df["num_clients"], proto_df[f"throughput"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
    plt.xlabel("clients per region")
    plt.ylabel("throughput (txns/s)")
    plt.legend(bbox_to_anchor=(0.5, 1.2), loc="upper center", ncol=3)
    plt.savefig(os.path.join(dir_path, f"rubis_throughput.png"), dpi=300)

def plot_rubis_unstable_ops(df, dir_path):
    df = df[df["datatype"] == "rubis"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    df = df[df["proto"] == "demon"]
    df = df[df["num_clients"] <= 4000]
    plt.figure(figsize=(7, 4))  # Adjust size as needed
    plt.plot(df["num_clients"], df["mean_unstable_ops_count"], PLOT_STYLES["demon"], color=COLORS["demon"])
    plt.xlabel("throughput (txns/s)")
    plt.ylabel("mean unstable operations count")
    plt.savefig(os.path.join(dir_path, f"rubis_unstable_ops_count.png"), dpi=300)

def plot_strong_ratio(df, dir_path):
    df = df[df["datatype"] == "non-neg-counter"]
    df = df[df["duration"] == 10]
    df = df[df["cluster_size"] == 5]
    df = df[df["num_clients"] == 100]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]
    for kind in ["client", "remote"]:
        plt.figure(figsize=(7, 4))  # Adjust size as needed
        for proto in protocols:
            proto_df = df[df["proto"] == proto]
            plt.plot(proto_df["strong_ratio"], proto_df[f"{kind}_mean_latency"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
        plt.xlabel("ratio of strong operations")
        plt.ylabel(f"mean {kind} latency (ms)")
        plt.legend()
        plt.savefig(os.path.join(dir_path, f"strong_ratio_{kind}_latency.png"), dpi=300)

def plot_rubis_violin(df, dir_path, duration=60, cluster_size=5, num_clients=None, limit=None):
    pass

def plot_conflict_histogram(dir_path):
    df = pd.read_csv(os.path.join(dir_path, "rubis_strict_5nodes_60s_4000clients_1strong_1keys.csv"))
    df["op"] = df["meta"].str.split(" ").str.get(1)
    init = df[df["kind"] == "initiated"]
    # client = init.merge(df[df["kind"] == "visible"][["meta", "node", "unix_micros"]], on=["meta", "node"], suffixes=("", "_client_visible"))
    # client["client_latency"] = (client["unix_micros_client_visible"] - client["unix_micros"]) / 1000

    last_visible = df[df["kind"] == "visible"].sort_values(by=["unix_micros"]).drop_duplicates(subset=["meta"], keep="last")
    remote = init.merge(last_visible[["meta", "unix_micros"]], on=["meta"], suffixes=("", "_end"))
    remote["remote_latency"] = (remote["unix_micros_end"] - remote["unix_micros"]) / 1000
    remote.index = pd.IntervalIndex.from_arrays(remote["unix_micros"], remote["unix_micros_end"])
    remote["conflicts"] = 0
    bids = remote.loc[remote["op"] == "Bid"]
    closeAuctions = remote.loc[remote["op"] == "CloseAuction"]

    i = 0
    for interval in closeAuctions.index.values:
        overlaps = bids.index.overlaps(interval).astype(int)
        closeAuctions["conflicts"].iloc[i] = overlaps.sum()
        bids["conflicts"] += overlaps
        i += 1

    plt.figure(figsize=(4, 4))  # Adjust size as needed
    plt.hist(bids["conflicts"])
    plt.xlabel("number of conflicts")
    plt.ylabel("bids")
    plt.savefig(os.path.join(dir_path, f"conflict_histogram_bids.png"), dpi=300)

    plt.figure(figsize=(4, 4))  # Adjust size as needed
    plt.hist(closeAuctions["conflicts"])
    plt.xlabel("number of conflicts")
    plt.ylabel("closeAuctions")
    plt.savefig(os.path.join(dir_path, f"conflict_histogram_closeauction.png"), dpi=300)


def plot_rubis_cumulative_latency_dist(dir_path):
    for kind in ["client", "remote"]:
        plt.figure(figsize=(4, 4))  # Adjust size as needed
        for proto in ["demon", "causal", "redblue", "gemini", "unistore", "strict"]:
            df = pd.read_csv(os.path.join(dir_path, f"rubis_{proto}_5nodes_60s_100clients_1strong_1keys.csv"))
            init = df[df["kind"] == "initiated"]
            if kind == "client":
                merged = init.merge(df[df["kind"] == "visible"][["meta", "node", "unix_micros"]], on=["meta", "node"], suffixes=("", "_end"))
            else:
                last_visible = df[df["kind"] == "visible"].sort_values(by=["unix_micros"]).drop_duplicates(subset=["meta"], keep="last")
                merged = init.merge(last_visible[["meta", "unix_micros"]], on=["meta"], suffixes=("", "_end"))
            merged["latency"] = (merged["unix_micros_end"] - merged["unix_micros"]) / 1000

            plt.ecdf(merged["latency"], label=LABELS[proto], color=COLORS[proto])
        plt.xscale("log")
        plt.xlabel("latency (ms)")
        plt.ylabel("probability")
        plt.savefig(os.path.join(dir_path, f"{kind}_latency_distribution.png"), dpi=300)


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

    df = aggregate(benches, path, recompute)
    if figure == "histogram":
        plot_conflict_histogram(path)
    elif figure == "latency-dist":
        plot_rubis_cumulative_latency_dist(path)
    elif figure == "rubis-lines":
        plot_rubis_lines(df, path)
    elif figure == "rubis-unstable":
        plot_rubis_unstable_ops(df, path)
    elif figure == "rubis-latency-bars":
        plot_latency_bars(df, path, datatype="rubis", num_clients=100, strong_ratio=1, duration=60, limit=2000, ops=["Bid", "CloseAuction", "BuyNow", "OpenAuction", "Sell", "RegisterUser"])
    elif figure == "strong-ratio":
        plot_strong_ratio(df, path)
    elif figure == "scaling":
        plot_scaling(df, path, datatype="rubis", duration=60, num_clients=2000)
    elif figure == "non-neg-latency-bars":
        plot_latency_bars(df, path, datatype="non-neg-counter", num_clients=100, strong_ratio=0.5, duration=10, ops=["Add", "Subtract"])
    elif figure == "co-editor-latency-bars":
        plot_latency_bars(df, path, datatype="co-editor", num_clients=1000, strong_ratio=0.001, ops=["Insert", "ChangeRole"], limit=2000)
    else:
        print("not a known figure name")

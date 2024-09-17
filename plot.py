import os
import sys
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import scipy.stats as st


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
    "demon": "semi-ser",
    "gemini": "gemini",
    "redblue": "redblue (new)",
    "unistore": "unistore",
    "causal": "causal",
    "strict": "strict",
}


def p99(series):
    return st.scoreatpercentile(series, 99.0)

def calc_stats(series, confidence_level=0.95):
    return {
        "mean": st.tmean(series),
        "mean_conf_int": st.bootstrap((series,), st.tmean, confidence_level=confidence_level, method="percentile").confidence_interval,
        "p99": p99(series),
        "p99_conf_int": st.bootstrap((series,), p99, confidence_level=confidence_level, method="percentile").confidence_interval,
    }

def plot_tpcc(df, dir_path):
    for cluster_size in df["cluster_size"].unique():
        cluster_df = df[df["cluster_size"] == cluster_size]
        protocols = cluster_df["proto"].unique()
        plt.figure(figsize=(10, 6))  # Adjust size as needed
        for proto in protocols:
            proto_df = cluster_df[cluster_df["proto"] == proto]
            plt.plot(proto_df["total_throughput"], (proto_df["total_time"] * 1000) / proto_df["total_count"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
        plt.xlabel("throughput (txns/s)")
        plt.ylabel("mean latency (ms)")
        plt.title(f"TPCC latency by throughput ({cluster_size} replicas)")
        plt.legend()
        plt.savefig(os.path.join(dir_path, f"tpcc_plot_{cluster_size}_nodes.png"), dpi=300)

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
    

def plot_micro(df, dir_path):
    for cluster_size in df["cluster_size"].unique():
        cluster_df = df[df["cluster_size"] == cluster_size]
        datatypes = cluster_df["datatype"].unique()
        for dtype in datatypes:
            plot_single_micro(cluster_df[cluster_df["datatype"] == dtype], dtype, cluster_size, dir_path)

def plot_single_micro(df, datatype, cluster_size, dir_path):
    protocols = df["proto"].unique()
    # aggregate the data
    throughputs = {}
    mean_latencies = {}
    p95_latencies = {}
    p99_latencies = {}
    for proto in protocols:
        proto_df = df[df["proto"] == proto]
        grouped = proto_df.groupby("strong_ratio")
        idx = grouped["total_throughput"].idxmax()
        rows_with_max_throughput = proto_df.loc[idx]
        print(rows_with_max_throughput[["num_clients", "proto", "datatype", "strong_ratio"]])
        throughputs[proto] = {
            "x": rows_with_max_throughput["strong_ratio"],
            "y": rows_with_max_throughput["total_throughput"],
        }
        mean_latencies[proto] = {
            "x": rows_with_max_throughput["strong_ratio"],
            "y": rows_with_max_throughput["total_mean_latency"],
        }
        p95_latencies[proto] = {
            "x": rows_with_max_throughput["strong_ratio"],
            "y": rows_with_max_throughput["total_p95_latency"],
        }
        p99_latencies[proto] = {
            "x": rows_with_max_throughput["strong_ratio"],
            "y": rows_with_max_throughput["total_p99_latency"],
        }

    plt.figure(figsize=(10, 10))  # Adjust size as needed
    plt.suptitle(f"{datatype} with {cluster_size} replicas")

    plt.subplot(2, 2, 1)
    for proto, vals in mean_latencies.items():
        plt.plot(vals["x"], vals["y"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
    plt.xlabel("strong operation ratio")
    plt.ylabel("mean latency (ms)")
    plt.title(f"mean latency at max throughput")
    plt.legend()

    plt.subplot(2, 2, 2)
    for proto, vals in throughputs.items():
        plt.plot(vals["x"], vals["y"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
    plt.yscale("log")
    plt.xlabel("strong operation ratio")
    plt.ylabel("throughput (ops/s)")
    plt.title(f"max throughput")
    plt.legend()

    plt.subplot(2, 2, 3)
    for proto, vals in p95_latencies.items():
        plt.plot(vals["x"], vals["y"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
    plt.xlabel("strong operation ratio")
    plt.ylabel("95th percentile latency (ms)")
    plt.title(f"95th percentile latency at max throughput")
    plt.legend()

    plt.subplot(2, 2, 4)
    for proto, vals in p99_latencies.items():
        plt.plot(vals["x"], vals["y"], PLOT_STYLES[proto], label=LABELS[proto], color=COLORS[proto])
    plt.xlabel("strong operation ratio")
    plt.ylabel("99th percentile latency (ms)")
    plt.title(f"99th percentile latency at max throughput")
    plt.legend()

    plt.savefig(os.path.join(dir_path, f"{datatype}_plot_{cluster_size}_nodes.png"), dpi=300)
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("argument required to specify input directory")
        exit()

    args = sys.argv[1:]
    path = args[0]
    
    try:
        df = pd.read_csv(os.path.join(path, "micro.csv"))
        plot_rubis(df, path)
        exit()
        plot_micro(df, path)
    except Exception as e:
        print(f"err: {e}")
    try:
        df = pd.read_csv(os.path.join(path, "tpcc.csv"))
        plot_tpcc(df, path)
    except Exception as e:
        print(f"err: {e}")

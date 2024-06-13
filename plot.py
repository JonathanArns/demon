import os
import sys
import matplotlib.pyplot as plt
import pandas as pd


def plot_tpcc(df, dir_path):
    for cluster_size in df["cluster_size"].unique():
        cluster_df = df[df["cluster_size"] == cluster_size]
        protocols = cluster_df["proto"].unique()
        plt.figure(figsize=(10, 6))  # Adjust size as needed
        for proto in protocols:
            proto_df = cluster_df[cluster_df["proto"] == proto]
            plt.plot(proto_df["total_throughput"], proto_df["total_mean_latency"], marker="o", label=proto)
        plt.xlabel("throughput (txns/s)")
        plt.ylabel("mean latency (ms)")
        plt.title(f"TPCC latency by throughput ({cluster_size} replicas)")
        plt.legend()
        plt.savefig(os.path.join(dir_path, f"tpcc_plot_{cluster_size}_nodes.png"), dpi=300)

def plot_micro(df, dir_path):
    for cluster_size in df["cluster_size"].unique():
        cluster_df = df[df["cluster_size"] == cluster_size]
        datatypes = cluster_df["datatype"].unique()
        for dtype in datatypes:
            plot_single_micro(cluster_df[cluster_df["datatype"] == dtype], dtype, cluster_size, dir_path)

def plot_single_micro(df, datatype, cluster_size, dir_path):
    protocols = df["proto"].unique()
    # aggregate the data
    mean_latencies = {}
    throughputs = {}
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

    plt.figure(figsize=(10, 6))  # Adjust size as needed
    plt.suptitle(f"{datatype} with {cluster_size} replicas")

    plt.subplot(1, 2, 1)
    for proto, vals in mean_latencies.items():
        plt.plot(vals["x"], vals["y"], marker="o", label=proto)
    plt.xlabel("strong operation ratio")
    plt.ylabel("mean latency (ms)")
    plt.title(f"{datatype} mean latency at max throughput")
    plt.legend()

    plt.subplot(1, 2, 2)
    for proto, vals in throughputs.items():
        plt.plot(vals["x"], vals["y"], marker="o", label=proto)
    plt.yscale("log")
    plt.xlabel("strong operation ratio")
    plt.ylabel("throughput (ops/s)")
    plt.title(f"{datatype} max throughput")
    plt.legend()

    plt.savefig(os.path.join(dir_path, f"{datatype}_plot_{cluster_size}_nodes.png"), dpi=300)
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("argument required to specify input directory")
        exit()

    args = sys.argv[1:]
    path = args[0]
    
    try:
        plot_micro(pd.read_csv(os.path.join(path, "micro_bench.csv")), path)
    except Exception as e:
        print(f"err: {e}")
    try:
        plot_tpcc(pd.read_csv(os.path.join(path, "tpcc.csv")), path)
    except Exception as e:
        print(f"err: {e}")

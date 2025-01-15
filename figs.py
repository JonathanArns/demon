import os
import sys

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerTuple
import pandas as pd
import numpy as np
import scipy.stats as st
import json

matplotlib.rcParams.update({'font.size': 20})
plt.rcParams['axes.axisbelow'] = True

PLOT_STYLES = {
    "demon": "o-",
    "gemini": "v-",
    "redblue": "^-",
    "unistore": ">-",
    "causal": "<-",
    "strict": "x-",
}
P99_PLOT_STYLES = {
    "demon": "o:",
    "gemini": "v:",
    "redblue": "^:",
    "unistore": ">:",
    "causal": "<:",
    "strict": "x:",
}
LINE_STYLE = {
    "demon": "solid",
    "gemini": (0, (5, 1, 1, 1, 1, 1)),
    "redblue": "dashed",
    "unistore": "dashdot",
    "causal": (0, (1, 3)),
    "strict": "dotted",
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
    "demon": "#377eb8",
    "gemini": "#984ea3",
    "redblue": "#e41a1c",
    "unistore": "#4daf4a",
    "causal": "#000000",
    "strict": "#ff7f00",
}

LABELS = {
    "demon": "DeMon",
    "gemini": "RedBlue (Gemini)",
    "redblue": "RedBlue (ft)",
    "unistore": "Optimistic PoR",
    "causal": "No guarantees",
    "strict": "Consensus",
}

PROTOS = ["demon", "redblue", "gemini", "unistore", "strict", "causal"]


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


def plot_latency_bars1(df, dir_path, datatype="", duration=60, cluster_size=5, num_clients=1000, strong_ratio=0.5, ops=[], limit=None):
    df = df[df["datatype"] == datatype]
    df = df[df["duration"] == duration]
    df = df[df["cluster_size"] == cluster_size]
    df = df[df["num_clients"] == num_clients]
    df = df[df["strong_ratio"] == strong_ratio]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]
    index = np.arange(len(ops))
    bar_width = 1

    # Create figure and 2 subplots
    if len(ops) == 2:
        fig, axs = plt.subplots(1, 2, figsize=(10, 5), constrained_layout=True)
    else:
        fig, axs = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
    kinds = ["client", "remote"]

    
    for i, kind in enumerate(kinds):
        ax = axs[i]
        ax.set_yscale('log', base=10)
        proto_index = 0  # Reset index for protocols
        artists = []
        labels = []
        for proto in protocols:
            proto_df = df[df["proto"] == proto]
            artists.append(ax.bar(
                x=index * (len(protocols) + 1) * bar_width + proto_index * bar_width,
                height=[proto_df[f"{op}_{kind}_median_latency"].iloc[0] for op in ops],
                yerr=([0 for _ in range(len(ops))], [proto_df[f"{op}_{kind}_p99_latency"].iloc[0] for op in ops]),
                width=bar_width,
                label=LABELS[proto],
                capsize=5.0,
                color=COLORS[proto],
                edgecolor="black",
                hatch=BAR_PATTERNS[proto],
            ))
            labels.append(LABELS[proto])
            proto_index += 1
        if kind == "remote":
            ax.set_ylim(10**2, None)
        ax.set_xticks([(x+0.5) * bar_width * (len(protocols) + 1) - 1 for x in range(len(ops))])
        ax.set_xticklabels(ops)
        ax.yaxis.set_major_locator(plt.LogLocator(base=10, numticks=10))
        ax.yaxis.set_minor_locator(plt.LogLocator(base=10, subs='all', numticks=100))
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"{kind}".capitalize())
        ax.grid(linestyle="--", linewidth=0.5, which="both", axis="y")

    lgd = plt.figlegend(artists, labels, loc="lower center", ncol=3, bbox_to_anchor=(0.5, 1.02))
    plt.savefig(os.path.join(dir_path, f"{datatype}-latency-bars.pdf"), bbox_extra_artists=(lgd,), bbox_inches="tight")

def plot_latency_bars2(df, dir_path, datatype="", duration=60, cluster_size=5, num_clients=1000, strong_ratio=0.5, ops=[], limit=None):
    df = df[df["datatype"] == datatype]
    df = df[df["duration"] == duration]
    df = df[df["cluster_size"] == cluster_size]
    df = df[df["num_clients"] == num_clients]
    df = df[df["strong_ratio"] == strong_ratio]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]
    index = np.arange(len(ops))
    bar_width = 1

    # Create figure and 2 subplots
    fig, axs = plt.subplots(2, 1, figsize=(12, 10))  # Adjust figure size to reduce whitespace
    kinds = ["client", "remote"]

    for i, kind in enumerate(kinds):
        ax = axs[i]
        ax.set_yscale('log', base=10)
        if limit:
            ax.set_ylim(None, limit)
        proto_index = 0  # Reset index for protocols
        for proto in protocols:
            # if kind == "remote":
            #     plt.ylim(100, 10**5)
            proto_df = df[df["proto"] == proto]
            ax.bar(
                x=index * (len(protocols) + 1) * bar_width + proto_index * bar_width,
                height=[proto_df[f"{op}_{kind}_median_latency"].iloc[0] for op in ops],
                yerr=([0 for _ in range(len(ops))], [proto_df[f"{op}_{kind}_p99_latency"].iloc[0] for op in ops]),
                width=bar_width,
                label=LABELS[proto],
                capsize=5.0,
                color=COLORS[proto],
                edgecolor="black",
                hatch=BAR_PATTERNS[proto],
            )
            proto_index += 1
        ax.set_xticks([(x+0.5) * bar_width * (len(protocols) + 1) - 1 for x in range(len(ops))])
        ax.set_xticklabels(ops)
        ax.yaxis.set_major_locator(plt.LogLocator(base=10, numticks=10))
        ax.yaxis.set_minor_locator(plt.LogLocator(base=10, subs='all', numticks=100))
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"{kind}".capitalize())
        ax.grid(linestyle="--", linewidth=0.5, which="both", axis="y")

    # Add the legend only to the lower plot
    axs[1].legend(bbox_to_anchor=(0.5, 1.15), loc="lower center", ncol=3)

    # Adjust spacing between subplots to reduce whitespace
    fig.subplots_adjust(hspace=0.1)  # Reduce space between subplots

    plt.tight_layout()
    plt.savefig(os.path.join(dir_path, f"{datatype}-latency-bars.pdf"))

def plot_scaling(df, dir_path, datatype="", duration=60, num_clients=2000, limit=None):
    df = df[df["proto"] == "demon"]
    df = df[df["datatype"] == datatype]
    df = df[df["duration"] == duration]
    df = df[df["num_clients"] == num_clients]
    cluster_sizes = [3, 5, 7]
    index = np.arange(len(cluster_sizes))
    bar_width = 1
    fig, axs = plt.subplots(1, 3, figsize=(12, 4), constrained_layout=True)
    for i, kind in enumerate(["client", "remote"]):
        ax = axs[i+1]
        ax.bar(
            x=index * bar_width,
            height=[df[df["cluster_size"] == size][f"{kind}_median_latency"].iloc[0] for size in cluster_sizes],
            yerr=([0 for x in range(len(cluster_sizes))], [df[df["cluster_size"] == size][f"{kind}_p99_latency"].iloc[0] for size in cluster_sizes]),
            width=bar_width,
            # label=LABELS["demon"],
            capsize=5.0,
            color=COLORS["demon"],
            edgecolor="black",
            # hatch=BAR_PATTERNS["demon"],
        )
        if kind == "client":
            ax.set_yscale("log", base=10)
        ax.grid(linestyle="--", linewidth=0.5, which="both", axis="y")
        ax.set_xticks([x for x in range(len(cluster_sizes))], cluster_sizes)
        ax.set_xlabel("#regions")
        ax.set_ylabel(f"Latency (ms)")
        ax.set_title(f"{kind.capitalize()} latency")

    ax = axs[0]
    ax.bar(
        x=index * bar_width,
        height=[df[df["cluster_size"] == size][f"throughput"].iloc[0] for size in cluster_sizes],
        width=bar_width,
        # label=LABELS["demon"],
        capsize=5.0,
        color=COLORS["demon"],
        edgecolor="black",
        # hatch=BAR_PATTERNS["demon"],
    )
    ax.grid(linestyle="--", linewidth=0.5, which="both", axis="y")
    ax.set_xticks([x for x in range(len(cluster_sizes))], cluster_sizes)
    ax.set_xlabel("#regions")
    ax.set_ylabel(f"Throughput (txn/s)")
    ax.set_title(f"Throughput")

    plt.savefig(os.path.join(dir_path, f"scaling.pdf"))


def plot_rubis_lines(df, dir_path):
    df = df[df["datatype"] == "rubis"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    df = df[df["key_range"] < 3]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]

    # Create a figure with 2 subplots, one below the other
    fig, axs = plt.subplots(2, 1, figsize=(12, 10))  # Reduced figure height
    kinds = ["client", "remote"]

    for i, kind in enumerate(kinds):
        ax = axs[i]
        if kind == "client":
            ax.set_ylim(10**-3, 10 ** 4)
        else:
            ax.set_ylim(10**2, 10 ** 4)
        ax.set_yscale('log', base=10)
        for proto in protocols:
            # if proto == "causal" and kind == "client":
            #     continue
            if proto == "demon":
                line_width = 2
            else:
                line_width = 1.5
            proto_df = df[df["proto"] == proto]
            ax.plot(proto_df["num_clients"], proto_df[f"{kind}_median_latency"], PLOT_STYLES[proto],
                    label=LABELS[proto] + " p50", color=COLORS[proto], linewidth=line_width)
            ax.plot(proto_df["num_clients"], proto_df[f"{kind}_p95_latency"], P99_PLOT_STYLES[proto],
                    label=LABELS[proto] + " p95", color=COLORS[proto], linewidth=line_width)
        ax.set_xlabel("Clients per region")
        ax.set_title(f"{kind}".capitalize())
        ax.set_ylabel("Latency (ms)")
        ax.grid(linestyle="--", linewidth=0.5, which="both", axis="y")

    # Add the legend only to the lower plot, in the position of the upper plot's original legend
    axs[1].legend(bbox_to_anchor=(0.5, 1.25), loc="upper center", ncol=6)  # Adjust ncol for all protocols

    # Adjust the space between subplots to reduce whitespace
    fig.subplots_adjust(hspace=0.05)  # Reduce vertical space between plots

    plt.tight_layout()
    plt.savefig(os.path.join(dir_path, f"rubis_latency.pdf"))


def plot_rubis_throughput(df, dir_path):
    df = df[df["datatype"] == "rubis"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    df = df[df["key_range"] < 3]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]

    plt.figure(figsize=(12, 5), constrained_layout=True)  # Adjust size as needed
    for proto in protocols:
        proto_df = df[df["proto"] == proto]
        if proto == "causal":
            style = ""
        else:
            style =  PLOT_STYLES[proto]
        plt.plot(proto_df["num_clients"], proto_df["throughput"], style,
                 label=LABELS[proto], color=COLORS[proto], linewidth=2.0, markersize=10.0)
    plt.xlabel("Clients per region")
    plt.ylabel("Throughput (K txns/s)")
    plt.yticks(ticks=[0, 2000, 4000, 6000, 8000, 10_000, 12_000, 14_000, 16_000],
               labels=["0", "2", "4", "6", "8", "10", "12", "14", "16"])
    plt.grid(linestyle="--", linewidth=0.5)
    lgd = plt.figlegend(loc="lower center", ncol=3, bbox_to_anchor=(0.5, 1.02))
    plt.savefig(os.path.join(dir_path, f"rubis_throughput.pdf"), bbox_extra_artists=(lgd,), bbox_inches="tight")

def plot_rubis_unstable_ops(df, dir_path):
    df = df[df["datatype"] == "rubis"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    df = df[df["proto"] == "demon"]
    df = df[df["num_clients"] <= 4000]
    plt.figure(figsize=(12, 5))  # Adjust size as needed
    # plt.ylim(0, 4100)
    plt.xlim(0, 4100)
    plt.plot(df["num_clients"], df["mean_unstable_ops_count"], PLOT_STYLES["demon"],
             color=COLORS["demon"], linewidth=2)
    plt.xlabel("Throughput (txns/s)")
    plt.ylabel("#Unstable Operations")
    plt.grid(linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(dir_path, "rubis_unstable_ops_count.pdf"))

def plot_strong_ratio(df, dir_path):
    df = df[df["datatype"] == "non-neg-counter"]
    df = df[df["duration"] == 10]
    df = df[df["cluster_size"] == 5]
    df = df[df["num_clients"] == 100]
    df = df[df["strong_ratio"] < 1.0]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]
    for kind in ["client", "remote"]:
        plt.figure(figsize=(12, 5), constrained_layout=True)
        plt.ylim(-20, 270)
        for proto in protocols:
            proto_df = df[df["proto"] == proto]
            plt.plot(proto_df["strong_ratio"], proto_df[f"{kind}_mean_latency"], PLOT_STYLES[proto],
                     label=LABELS[proto], color=COLORS[proto], linewidth=2.0, markersize=10.0)
        plt.xlabel("Ratio of strong operations")
        plt.ylabel(f"Mean latency (ms)")
        plt.grid(linestyle="--", linewidth=0.5)
        lgd = plt.figlegend(loc="lower center", ncol=3, bbox_to_anchor=(0.5, 1.02))
        plt.savefig(os.path.join(dir_path, f"strong_ratio_{kind}_latency.pdf"), bbox_extra_artists=(lgd,), bbox_inches="tight")

def plot_rubis_cumulative_latency_dist(dir_path):
    fig, axs = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
    for i, kind in enumerate(["client", "remote"]):
        ax = axs[i]
        artists = []
        labels = []
        for proto in ["demon", "causal", "redblue", "gemini", "unistore", "strict"]:
            df = pd.read_csv(os.path.join(dir_path, f"rubis_{proto}_5nodes_60s_100clients_1strong_1keys.csv"))
            init = df[df["kind"] == "initiated"]
            if kind == "client":
                merged = init.merge(df[df["kind"] == "visible"][["meta", "node", "unix_micros"]], on=["meta", "node"], suffixes=("", "_end"))
            else:
                last_visible = df[df["kind"] == "visible"].sort_values(by=["unix_micros"]).drop_duplicates(subset=["meta"], keep="last")
                merged = init.merge(last_visible[["meta", "unix_micros"]], on=["meta"], suffixes=("", "_end"))
            merged["latency"] = (merged["unix_micros_end"] - merged["unix_micros"]) / 1000

            artists.append(ax.ecdf(merged["latency"], label=LABELS[proto], color=COLORS[proto], linestyle=LINE_STYLE[proto], linewidth=2.0))
            labels.append(LABELS[proto])
        ax.grid(linestyle="--", linewidth=0.5, which="major")
        ax.set_xscale("log", base=10)
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("Probability")
        ax.set_title(kind.capitalize())
    lgd = plt.figlegend(artists, labels, loc="lower center", ncol=3, bbox_to_anchor=(0.5, 1.02))
    plt.savefig(os.path.join(dir_path, f"latency_distribution.pdf"), bbox_extra_artists=(lgd,), bbox_inches="tight")


def plot_rubis_client_latency(df, dir_path):
    ops=["Bid", "CloseAuction", "BuyNow", "OpenAuction", "Sell", "RegisterUser"]
    df = df[df["datatype"] == "rubis"]
    df = df[df["duration"] == 60]
    df = df[df["cluster_size"] == 5]
    df = df[df["num_clients"] == 100]
    df = df[df["strong_ratio"] == 1]
    protocols = [proto for proto in PROTOS if proto in df["proto"].unique()]
    index = np.arange(len(ops))
    bar_width = 1

    fig, axs = plt.subplots(1, 2, width_ratios=(4, 9), figsize=(24, 5), constrained_layout=True)
    artists = []

    ax = axs[1]
    ax.set_yscale('log', base=10)
    proto_index = 0  # Reset index for protocols
    for proto in protocols:
        proto_df = df[df["proto"] == proto]
        artist = ax.bar(
            x=index * (len(protocols) + 1) * bar_width + proto_index * bar_width,
            height=[proto_df[f"{op}_client_median_latency"].iloc[0] for op in ops],
            yerr=([0 for _ in range(len(ops))], [proto_df[f"{op}_client_p99_latency"].iloc[0] for op in ops]),
            width=bar_width,
            label=LABELS[proto],
            capsize=5.0,
            color=COLORS[proto],
            edgecolor="black",
            hatch=BAR_PATTERNS[proto],
        )
        artists.append(artist)
        proto_index += 1
    ax.set_xticks([(x+0.5) * bar_width * (len(protocols) + 1) - 1 for x in range(len(ops))])
    ax.set_xticklabels(ops)
    ax.yaxis.set_major_locator(plt.LogLocator(base=10, numticks=10))
    ax.yaxis.set_minor_locator(plt.LogLocator(base=10, subs='all', numticks=100))
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"Median & 99th percentile latency per operation")
    ax.grid(linestyle="--", linewidth=0.5, which="both", axis="y")

    ax = axs[0]
    labels = []
    for i, proto in enumerate(protocols):
        df = pd.read_csv(os.path.join(dir_path, f"rubis_{proto}_5nodes_60s_100clients_1strong_1keys.csv"))
        init = df[df["kind"] == "initiated"]
        merged = init.merge(df[df["kind"] == "visible"][["meta", "node", "unix_micros"]], on=["meta", "node"], suffixes=("", "_end"))
        merged["latency"] = (merged["unix_micros_end"] - merged["unix_micros"]) / 1000
        artist = ax.ecdf(merged["latency"], label=LABELS[proto], color=COLORS[proto], linestyle=LINE_STYLE[proto], linewidth=2.0)
        artists[i] = (artists[i], artist)
        labels.append(LABELS[proto])
    ax.grid(linestyle="--", linewidth=0.5, which="major")
    ax.set_xscale("log", base=10)
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Probability")
    ax.set_title("Cumulative latency distribution")

    lgd = plt.figlegend(artists, labels, handler_map={tuple: HandlerTuple(ndivide=None)}, handlelength=4.0, loc="lower center", ncol=6, bbox_to_anchor=(0.5, 1.0))
    plt.savefig(os.path.join(dir_path, f"rubis_client_latency.pdf"), bbox_extra_artists=(lgd,), bbox_inches="tight")


def plot_workload_classification(name):
    if name == "rubis":
        ops = ["Bid", "BuyNow", "RegisterUser", "Sell", "OpenAuction", "CloseAuction"]
        percentage = [60, 13, 7, 8, 8, 4]
        is_strong = {
            "DeMon": [False, True, True, False, False, True],
            "RedBlue": [True, True, True, False, False, True],
        }

    plt.figure(figsize=(12, 5), constrained_layout=True)
    for i, op in enumerate(ops):
        plt.barh()


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

    if figure == "latency-dist":
        plot_rubis_cumulative_latency_dist(path)
    elif figure == "client-latency-dist":
        plot_rubis_cumulative_client_latency_dist(path)
    else:
        df = aggregate(path, recompute)
        if figure == "rubis-lines":
            plot_rubis_lines(df, path)
        elif figure == "rubis-client-latency":
            plot_rubis_client_latency(df, path)
        elif figure == "rubis-throughput":
            plot_rubis_throughput(df, path)
        elif figure == "rubis-unstable":
            plot_rubis_unstable_ops(df, path)
        elif figure == "rubis-latency-bars":
            plot_latency_bars2(df, path, datatype="rubis", num_clients=100, strong_ratio=1, duration=60, ops=["Bid", "CloseAuction", "BuyNow", "OpenAuction", "Sell", "RegisterUser"])
        elif figure == "strong-ratio":
            plot_strong_ratio(df, path)
        elif figure == "scaling":
            plot_scaling(df, path, datatype="rubis", duration=60, num_clients=2000)
        elif figure == "non-neg-latency-bars":
            plot_latency_bars1(df, path, datatype="non-neg-counter", num_clients=100, strong_ratio=0.5, duration=10, ops=["Add", "Subtract"])
        elif figure == "co-editor-latency-bars":
            plot_latency_bars1(df, path, datatype="co-editor", num_clients=1000, strong_ratio=0.001, ops=["Insert", "Delete", "ChangeRole"])
        else:
            print("not a known figure name")

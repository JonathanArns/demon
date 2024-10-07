import os
import sys
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import random

NUM_AUCTIONS = 100
NUM_CLIENTS = 1000
LATENCY = 200
DURATION = 100


key_offset = 0
def close_auction():
    global key_offset
    key_offset += 1
    return key_offset - 1

def bid():
    global key_offset
    id = random.randint(0 + key_offset, NUM_AUCTIONS + key_offset)
    return id

def simulate():
    bids = []
    bid_intervals = []
    close_auctions = []
    close_auction_intervals = []

    client_times = [random.randint(0, 1000 * 1000) for _ in range(NUM_CLIENTS)]
    keep_running = True
    while keep_running:
        keep_running = False
        for i in range(NUM_CLIENTS):
            t = client_times[i]
            if t < DURATION * 1000 * 1000:
                keep_running = True
                if random.random() < 0.9375:
                    bids.append(bid())
                    bid_intervals.append((t, t+(LATENCY*1000)))
                else:
                    close_auctions.append(close_auction())
                    close_auction_intervals.append((t, t+(LATENCY*1000)))
                client_times[i] += (LATENCY + 1000) * 1000
    
    bids = pd.DataFrame(bids, index=pd.IntervalIndex.from_tuples(bid_intervals), columns=["id"])
    close_auctions = pd.DataFrame(close_auctions, index=pd.IntervalIndex.from_tuples(close_auction_intervals), columns=["id"])
    return bids, close_auctions


def simulate_and_plot(dir_path):
    bids, close_auctions = simulate()
    bids["conflicts"] = 0
    close_auctions["conflicts"] = 0

    for interval in close_auctions.index.values:
        auction_id = close_auctions.loc[interval, "id"]
        overlaps = bids.index.overlaps(interval)
        conflicts = overlaps & (bids["id"] == auction_id)
        close_auctions.loc[interval, "conflicts"] = conflicts.astype(int).sum()
        bids["conflicts"] += conflicts.astype(int)

    plt.figure(figsize=(3, 5), constrained_layout=True)
    bins = sorted(bids["conflicts"].unique()) + [bids["conflicts"].max() + 1]
    plt.hist(bids["conflicts"], bins=bins, histtype="stepfilled", align="left")
    plt.xticks(bins[:-1])
    plt.xlabel("number of conflicts")
    plt.ylabel("bids")
    plt.savefig(os.path.join(dir_path, f"conflict_histogram_bids.pdf"), dpi=300)

    plt.figure(figsize=(5, 5), constrained_layout=True)
    bins = sorted(close_auctions["conflicts"].unique()) + [close_auctions["conflicts"].max() + 1]
    plt.hist(close_auctions["conflicts"], bins=bins, histtype="stepfilled", align="left")
    plt.xticks(bins[:-1])
    plt.xlabel("number of conflicts")
    plt.ylabel("closeAuctions")
    plt.savefig(os.path.join(dir_path, f"conflict_histogram_closeauction.pdf"), dpi=300)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("argument required to specify input directory")
        exit()

    path = sys.argv[1]
    try:
        os.mkdir(path)
    except Exception as e:
        pass
    simulate_and_plot(path)

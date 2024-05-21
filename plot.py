import matplotlib.pyplot as plt
import pandas as pd

# Load CSV file into a DataFrame
df = pd.read_csv('./experiment_output.csv')

# Extract data from columns
non_neg_counter_df = df[df['datatype'] == 'non-neg-counter']
strong_ratio = non_neg_counter_df[non_neg_counter_df['proto'] == 'demon']['strong_ratio']
latency_demon = non_neg_counter_df[non_neg_counter_df['proto'] == 'demon']['total_mean_latency']
latency_redblue = non_neg_counter_df[non_neg_counter_df['proto'] == 'redblue']['total_mean_latency']
latency_strict = non_neg_counter_df[non_neg_counter_df['proto'] == 'strict']['total_mean_latency']
latency_causal = non_neg_counter_df[non_neg_counter_df['proto'] == 'causal']['total_mean_latency']

throughput_demon = non_neg_counter_df[non_neg_counter_df['proto'] == 'demon']['total_throughput']
throughput_redblue = non_neg_counter_df[non_neg_counter_df['proto'] == 'redblue']['total_throughput']
throughput_strict = non_neg_counter_df[non_neg_counter_df['proto'] == 'strict']['total_throughput']
throughput_causal = non_neg_counter_df[non_neg_counter_df['proto'] == 'causal']['total_throughput']


# Plotting
plt.figure(figsize=(10, 6))  # Adjust size as needed
plt.suptitle('non-negative counter (3 replicas, 20ms round-trip between replicas, 10 clients per replica)')


plt.subplot(1, 2, 1)
plt.plot(strong_ratio, latency_demon, marker='o', label='semi-serializable')
plt.plot(strong_ratio, latency_redblue, marker='o', label='RedBlue')
plt.plot(strong_ratio, latency_strict, marker='o', label='strict')
plt.plot(strong_ratio, latency_causal, marker='o', label='causal')
plt.xlabel('strong operation (subtract) ratio')
plt.ylabel('mean latency (ms)')
plt.title('latency')
plt.legend()


plt.subplot(1, 2, 2)
plt.plot(strong_ratio, throughput_demon, marker='o', label='semi-serializable')
plt.plot(strong_ratio, throughput_redblue, marker='o', label='RedBlue')
plt.plot(strong_ratio, throughput_strict, marker='o', label='strict')
plt.plot(strong_ratio, throughput_causal, marker='o', label='causal')
plt.yscale('log')
plt.xlabel('strong operation (subtract) ratio')
plt.ylabel('throughput (ops/s)')
plt.title('throughput')

# TODO: use max throughput instead of at a random client count

plt.legend()

plt.savefig('line_plot.png', dpi=300)

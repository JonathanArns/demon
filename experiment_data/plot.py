import matplotlib.pyplot as plt
import pandas as pd

# Load CSV file into a DataFrame
df = pd.read_csv('./non_neg_counter_latency.csv')
print(df.dtypes)

# Extract data from columns
x = df['subtract_ratio']
y1 = df['semi_serializable']
y2 = df['RedBlue'] 
y3 = df['strict']

# Plotting
plt.figure(figsize=(10, 6))  # Adjust size as needed
plt.plot(x, y1, marker='o', label='semi-serializable')
plt.plot(x, y2, marker='o', label='RedBlue')
plt.plot(x, y3, marker='o', label='strict')

# Adding labels and title
plt.xlabel('subtract ratio')
plt.ylabel('latency (ms)')
plt.title('non-negative counter (20ms round-trip)')

# Adding legend
plt.legend()

plt.savefig('line_plot.png', dpi=300)

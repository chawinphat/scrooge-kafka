import pandas as pd
import matplotlib.pyplot as plt

# Load data from CSV
data = pd.read_csv('results.csv')

# Combine local and foreign network sizes into one column for simplicity
data['network_size'] = data['local_network_size']  # Assuming local_network_size == foreign_network_size

# Group by message size and network size to find average throughput for clarity in plotting
grouped_data = data.groupby(['message_size', 'network_size']).agg({'throughput': 'mean'}).reset_index()

# Get unique message sizes to plot each
message_sizes = grouped_data['message_size'].unique()

# Create a plot for each message size
for message_size in message_sizes:
    subset = grouped_data[grouped_data['message_size'] == message_size]
    plt.figure()
    plt.plot(subset['network_size'], subset['throughput'], marker='o', linestyle='-')
    plt.title(f'Throughput vs Network Size for Message Size: {message_size}')
    plt.xlabel('Network Size')
    plt.ylabel('Throughput')
    plt.grid(True)
    plt.savefig(f'throughput_vs_network_size_{message_size}.png')  # Save the figure as PNG
    plt.show()


#!/usr/bin/env python3
"""
Script to read metrics.jsonl and plot values over time.
Outputs a markdown file with base64 embedded graph.
"""

import json
import sys
import matplotlib.pyplot as plt
from datetime import datetime
import base64
from io import BytesIO

def read_metrics(jsonl_file):
    """Read JSONL file and extract RSS max, CPU max, latency, throughput, and timestamp data."""
    timestamps = []
    rss_max_values = []
    cpu_max_values = []
    latency_avg_values = []
    latency_max_values = []
    throughput_values = []  # MB/s
    msg_rate_values = []  # messages/s
    # Per-window flag: True only for windows inside the measured interval, i.e.
    # measurement_start_ms is set (not "-inf") and measurement_end_ms is "-inf".
    measured_flags = []

    # Store raw data for rolling window calculation
    raw_data = []
    
    with open(jsonl_file, 'r') as f:
        for line in f:
            if line.strip():
                data = json.loads(line)
                raw_data.append(data)
    
    # Process each data point
    total_bytes_sent = 0
    total_messages_sent = 0
    measurement_start_ms = None
    measurement_end_ms = None
    for i, data in enumerate(raw_data):
        # Extract window_end_ms timestamp
        timestamp_ms = int(data.get('window_end_ms', 0))
        
        # Extract RSS max value (it's a string, so convert to int)
        rss_max = data.get('rss', {}).get('max', '0')
        if rss_max == '-inf':
            rss_max = 0.0
        else:
            rss_max = float(rss_max)
        
        # Extract CPU max value
        cpu_max = data.get('cpu', {}).get('max', '0')
        if cpu_max == '-inf':
            cpu_max = 0
        else:
            cpu_max = float(cpu_max)
        
        # Extract latency average and max values
        latency_avg = data.get('latency', {}).get('average', '0')
        latency_max = data.get('latency', {}).get('max', '0')
        bytes_sent_data = float(data.get('bytes', {}).get('total', '0'))
        messages_sent_data = float(data.get('messages', {}).get('total', '0'))
        measurement_start_data_ms = data.get('measurement_start_ms', "-inf")
        measurement_end_data_ms = data.get('measurement_end_ms', "-inf")

        if measurement_end_data_ms != '-inf' and measurement_end_ms is None:
            measurement_end_ms = int(measurement_end_data_ms)
        if measurement_start_data_ms != '-inf' and not measurement_end_ms:
            if measurement_start_ms is None:
                measurement_start_ms = int(measurement_start_data_ms)
            total_bytes_sent += bytes_sent_data
            total_messages_sent += messages_sent_data

        
        if latency_avg == '-inf' or latency_avg == 0:
            latency_avg = 0
        else:
            latency_avg = float(latency_avg)
            
        if latency_max == '-inf' or latency_max == 0:
            latency_max = 0
        else:
            latency_max = float(latency_max)
               
        if measurement_start_ms is None or measurement_end_ms is not None:
            throughput_mbs = 0
            msg_rate = 0
        else:
            total_bytes_sent_mb = total_bytes_sent / (1024 * 1024)
            diff_s = (timestamp_ms - measurement_start_ms) / 1000.0
            throughput_mbs = total_bytes_sent_mb / diff_s if diff_s > 0 else 0
            msg_rate = total_messages_sent / diff_s if diff_s > 0 else 0
        
        timestamps.append(timestamp_ms)
        rss_max_values.append(rss_max)
        cpu_max_values.append(cpu_max)
        latency_avg_values.append(latency_avg)
        latency_max_values.append(latency_max)
        throughput_values.append(throughput_mbs)
        msg_rate_values.append(msg_rate)
        measured_flags.append(
            measurement_start_data_ms != '-inf' and measurement_end_data_ms == '-inf'
        )

    # Calculate efficiency metrics
    cpu_efficiency_values = []
    memory_efficiency_values = []
    
    for i in range(len(timestamps)):
        # CPU Efficiency: msg/(s * 1% CPU)
        if cpu_max_values[i] > 0:
            cpu_eff = msg_rate_values[i] / cpu_max_values[i]
        else:
            cpu_eff = 0
        cpu_efficiency_values.append(cpu_eff)
        
        # Memory Efficiency: msg/(s * 1KiB RSS)
        rss_kib = rss_max_values[i] / 1024
        if rss_kib > 0:
            mem_eff = msg_rate_values[i] / rss_kib
        else:
            mem_eff = 0
        memory_efficiency_values.append(mem_eff)
    
    return timestamps, rss_max_values, cpu_max_values, latency_avg_values, latency_max_values, throughput_values, msg_rate_values, cpu_efficiency_values, memory_efficiency_values, measured_flags

def plot_rss_metrics(timestamps, rss_max_values):
    """Create a matplotlib plot of RSS max values over time."""
    # Convert timestamps to relative seconds for better readability
    if timestamps:
        start_time = timestamps[0]
        relative_times = [(ts - start_time) / 1000.0 for ts in timestamps]
    else:
        relative_times = []
    
    # Convert bytes to MB for better readability
    rss_max_mb = [rss / (1024 * 1024) for rss in rss_max_values]
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(relative_times, rss_max_mb, marker='o', linestyle='-', linewidth=1, markersize=3)
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('RSS Max (MB)', fontsize=12)
    plt.title('RSS (Resident Set Size) Max Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot to base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()
    
    return image_base64

def plot_cpu_metrics(timestamps, cpu_max_values):
    """Create a matplotlib plot of CPU max values over time."""
    # Convert timestamps to relative seconds for better readability
    if timestamps:
        start_time = timestamps[0]
        relative_times = [(ts - start_time) / 1000.0 for ts in timestamps]
    else:
        relative_times = []
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(relative_times, cpu_max_values, marker='o', linestyle='-', linewidth=1, markersize=3, color='orange')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('CPU Max (%)', fontsize=12)
    plt.title('CPU Max Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot to base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()
    
    return image_base64

def plot_latency_metrics(timestamps, latency_avg_values, latency_max_values):
    """Create a matplotlib plot of latency average and max values over time."""
    # Convert timestamps to relative seconds for better readability
    if timestamps:
        start_time = timestamps[0]
        relative_times = [(ts - start_time) / 1000.0 for ts in timestamps]
    else:
        relative_times = []
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(relative_times, latency_avg_values, marker='o', linestyle='-', linewidth=1, markersize=3, color='green', label='Average Latency')
    plt.plot(relative_times, latency_max_values, marker='s', linestyle='-', linewidth=1, markersize=3, color='red', label='Max Latency')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Latency (ms)', fontsize=12)
    plt.title('Latency Over Time', fontsize=14, fontweight='bold')
    plt.legend(loc='best')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot to base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()
    
    return image_base64

def plot_throughput_metrics(timestamps, throughput_values):
    """Create a matplotlib plot of throughput (MB/s) over time."""
    # Convert timestamps to relative seconds for better readability
    if timestamps:
        start_time = timestamps[0]
        relative_times = [(ts - start_time) / 1000.0 for ts in timestamps]
    else:
        relative_times = []
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(relative_times, throughput_values, marker='o', linestyle='-', linewidth=1, markersize=3, color='purple')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Throughput (MB/s)', fontsize=12)
    plt.title('Throughput Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot to base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()
    
    return image_base64

def plot_msg_rate_metrics(timestamps, msg_rate_values):
    """Create a matplotlib plot of message rate (msg/s) over time."""
    # Convert timestamps to relative seconds for better readability
    if timestamps:
        start_time = timestamps[0]
        relative_times = [(ts - start_time) / 1000.0 for ts in timestamps]
    else:
        relative_times = []
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(relative_times, msg_rate_values, marker='o', linestyle='-', linewidth=1, markersize=3, color='brown')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Message Rate (msg/s)', fontsize=12)
    plt.title('Message Rate Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot to base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()
    
    return image_base64

def plot_cpu_efficiency_metrics(timestamps, cpu_efficiency_values):
    """Create a matplotlib plot of CPU efficiency (msg/(s*1% CPU)) over time."""
    # Convert timestamps to relative seconds for better readability
    if timestamps:
        start_time = timestamps[0]
        relative_times = [(ts - start_time) / 1000.0 for ts in timestamps]
    else:
        relative_times = []
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(relative_times, cpu_efficiency_values, marker='o', linestyle='-', linewidth=1, markersize=3, color='teal')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('CPU Efficiency (msg/(s*1% CPU))', fontsize=12)
    plt.title('CPU Efficiency Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot to base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()
    
    return image_base64

def plot_memory_efficiency_metrics(timestamps, memory_efficiency_values):
    """Create a matplotlib plot of memory efficiency (msg/(s*1MiB RSS)) over time."""
    # Convert timestamps to relative seconds for better readability
    if timestamps:
        start_time = timestamps[0]
        relative_times = [(ts - start_time) / 1000.0 for ts in timestamps]
    else:
        relative_times = []
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(relative_times, memory_efficiency_values, marker='o', linestyle='-', linewidth=1, markersize=3, color='darkviolet')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Memory Efficiency (msg/(s*1KiB RSS))', fontsize=12)
    plt.title('Memory Efficiency Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot to base64
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
    buffer.seek(0)
    image_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()
    
    return image_base64

def create_markdown_report(rss_image_base64, cpu_image_base64, latency_image_base64, throughput_image_base64, msg_rate_image_base64, cpu_efficiency_image_base64, memory_efficiency_image_base64, timestamps, rss_max_values, cpu_max_values, latency_avg_values, latency_max_values, throughput_values, msg_rate_values, cpu_efficiency_values, memory_efficiency_values, measured_flags):
    """Create a markdown report with embedded base64 image."""
    # Restrict the overall statistics and data-point counts to the measured
    # interval only: windows where measurement_start_ms is set (not "-inf") and
    # measurement_end_ms is still "-inf". Warmup/pre-measurement windows and the
    # closing marker (plus any trailing windows) are excluded. The plotted time
    # series above still show every window.
    def _measured(values):
        return [v for v, keep in zip(values, measured_flags) if keep]
    rss_max_values = _measured(rss_max_values)
    cpu_max_values = _measured(cpu_max_values)
    latency_avg_values = _measured(latency_avg_values)
    latency_max_values = _measured(latency_max_values)
    throughput_values = _measured(throughput_values)
    msg_rate_values = _measured(msg_rate_values)
    cpu_efficiency_values = _measured(cpu_efficiency_values)
    memory_efficiency_values = _measured(memory_efficiency_values)

    # Calculate some statistics
    # Calculate RSS statistics
    if rss_max_values:
        max_rss = max(rss_max_values) / (1024 * 1024)
        min_rss = min([r for r in rss_max_values if r > 0]) / (1024 * 1024) if any(r > 0 for r in rss_max_values) else 0
        avg_rss = sum(rss_max_values) / len(rss_max_values) / (1024 * 1024)
    else:
        max_rss = min_rss = avg_rss = 0
    
    # Calculate CPU statistics
    if cpu_max_values:
        max_cpu = max(cpu_max_values)
        min_cpu = min([c for c in cpu_max_values if c > 0]) if any(c > 0 for c in cpu_max_values) else 0
        avg_cpu = sum(cpu_max_values) / len(cpu_max_values)
    else:
        max_cpu = min_cpu = avg_cpu = 0
    
    # Calculate latency statistics
    if latency_avg_values:
        max_latency_avg = max(latency_avg_values)
        min_latency_avg = min([l for l in latency_avg_values if l > 0]) if any(l > 0 for l in latency_avg_values) else 0
        avg_latency_avg = sum(latency_avg_values) / len(latency_avg_values)
    else:
        max_latency_avg = min_latency_avg = avg_latency_avg = 0
    
    if latency_max_values:
        max_latency_max = max(latency_max_values)
        min_latency_max = min([l for l in latency_max_values if l > 0]) if any(l > 0 for l in latency_max_values) else 0
        avg_latency_max = sum(latency_max_values) / len(latency_max_values)
    else:
        max_latency_max = min_latency_max = avg_latency_max = 0
    
    # Calculate throughput statistics
    if throughput_values:
        max_throughput = max(throughput_values)
        min_throughput = min([t for t in throughput_values if t > 0]) if any(t > 0 for t in throughput_values) else 0
        avg_throughput = sum(throughput_values) / len(throughput_values)
    else:
        max_throughput = min_throughput = avg_throughput = 0
    
    # Calculate message rate statistics
    if msg_rate_values:
        max_msg_rate = max(msg_rate_values)
        min_msg_rate = min([m for m in msg_rate_values if m > 0]) if any(m > 0 for m in msg_rate_values) else 0
        avg_msg_rate = sum(msg_rate_values) / len(msg_rate_values)
    else:
        max_msg_rate = min_msg_rate = avg_msg_rate = 0
    
    # Calculate CPU efficiency statistics
    if cpu_efficiency_values:
        max_cpu_eff = max(cpu_efficiency_values)
        min_cpu_eff = min([c for c in cpu_efficiency_values if c > 0]) if any(c > 0 for c in cpu_efficiency_values) else 0
        avg_cpu_eff = sum(cpu_efficiency_values) / len(cpu_efficiency_values)
    else:
        max_cpu_eff = min_cpu_eff = avg_cpu_eff = 0
    
    # Calculate memory efficiency statistics
    if memory_efficiency_values:
        max_mem_eff = max(memory_efficiency_values)
        min_mem_eff = min([m for m in memory_efficiency_values if m > 0]) if any(m > 0 for m in memory_efficiency_values) else 0
        avg_mem_eff = sum(memory_efficiency_values) / len(memory_efficiency_values)
    else:
        max_mem_eff = min_mem_eff = avg_mem_eff = 0
    
    markdown_content = f"""# Performance Metrics Report

## Overview

Performance test metrics over time.

## RSS (Resident Set Size)

![RSS Max Over Time](data:image/png;base64,{rss_image_base64})

### RSS Statistics

- **Maximum RSS**: {max_rss:.2f} MB
- **Minimum RSS**: {min_rss:.2f} MB
- **Average RSS**: {avg_rss:.2f} MB
- **Total Data Points**: {len(rss_max_values)}

## CPU Usage

![CPU Max Over Time](data:image/png;base64,{cpu_image_base64})

### CPU Statistics

- **Maximum CPU**: {max_cpu:.2f}%
- **Minimum CPU**: {min_cpu:.2f}%
- **Average CPU**: {avg_cpu:.2f}%
- **Total Data Points**: {len(cpu_max_values)}

## Latency

![Latency Over Time](data:image/png;base64,{latency_image_base64})

### Latency Statistics

#### Average Latency
- **Maximum**: {max_latency_avg:.2f} ms
- **Minimum**: {min_latency_avg:.2f} ms
- **Average**: {avg_latency_avg:.2f} ms

#### Max Latency
- **Maximum**: {max_latency_max:.2f} ms
- **Minimum**: {min_latency_max:.2f} ms
- **Average**: {avg_latency_max:.2f} ms

- **Total Data Points**: {len(latency_avg_values)}

## Throughput

![Throughput Over Time](data:image/png;base64,{throughput_image_base64})

### Throughput Statistics

- **Maximum**: {max_throughput:.2f} MB/s
- **Minimum**: {min_throughput:.2f} MB/s
- **Average**: {avg_throughput:.2f} MB/s
- **Total Data Points**: {len(throughput_values)}

## Message Rate

![Message Rate Over Time](data:image/png;base64,{msg_rate_image_base64})

### Message Rate Statistics

- **Maximum**: {max_msg_rate:.2f} msg/s
- **Minimum**: {min_msg_rate:.2f} msg/s
- **Average**: {avg_msg_rate:.2f} msg/s
- **Total Data Points**: {len(msg_rate_values)}

## CPU Efficiency

![CPU Efficiency Over Time](data:image/png;base64,{cpu_efficiency_image_base64})

### CPU Efficiency Statistics

- **Maximum**: {max_cpu_eff:.2f} msg/(s*1% CPU)
- **Minimum**: {min_cpu_eff:.2f} msg/(s*1% CPU)
- **Average**: {avg_cpu_eff:.2f} msg/(s*1% CPU)
- **Total Data Points**: {len(cpu_efficiency_values)}

## Memory Efficiency

![Memory Efficiency Over Time](data:image/png;base64,{memory_efficiency_image_base64})

### Memory Efficiency Statistics

- **Maximum**: {max_mem_eff:.2f} msg/(s*1KiB RSS)
- **Minimum**: {min_mem_eff:.2f} msg/(s*1KiB RSS)
- **Average**: {avg_mem_eff:.2f} msg/(s*1KiB RSS)
- **Total Data Points**: {len(memory_efficiency_values)}

## Notes

- X-axis shows relative time in seconds from the start of the test
- RSS (Resident Set Size) represents the portion of memory occupied by a process that is held in RAM
- CPU values represent the percentage of CPU usage
- Latency values are in milliseconds (ms)
- Throughput is calculated as sum of bytes.total / measurement_time since start of measurement
- Message rate is calculated as sum of messages.total / measurement_time since start of measurement
- CPU Efficiency shows how many messages per second are processed per 1% CPU usage
- Memory Efficiency shows how many messages per second are processed per 1 KiB of RSS
"""
    
    return markdown_content

def main():
    jsonl_file = 'metrics.jsonl'
    output_file = 'metric_report.md'
    if len(sys.argv) > 1:
        jsonl_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    print(f"Reading metrics from {jsonl_file}...")
    timestamps, rss_max_values, cpu_max_values, latency_avg_values, latency_max_values, throughput_values, msg_rate_values, cpu_efficiency_values, memory_efficiency_values, measured_flags = read_metrics(jsonl_file)
    
    print(f"Found {len(timestamps)} data points")
    print("Creating RSS plot...")
    rss_image_base64 = plot_rss_metrics(timestamps, rss_max_values)
    
    print("Creating CPU plot...")
    cpu_image_base64 = plot_cpu_metrics(timestamps, cpu_max_values)
    
    print("Creating Latency plot...")
    latency_image_base64 = plot_latency_metrics(timestamps, latency_avg_values, latency_max_values)
    
    print("Creating Throughput plot...")
    throughput_image_base64 = plot_throughput_metrics(timestamps, throughput_values)
    
    print("Creating Message Rate plot...")
    msg_rate_image_base64 = plot_msg_rate_metrics(timestamps, msg_rate_values)
    
    print("Creating CPU Efficiency plot...")
    cpu_efficiency_image_base64 = plot_cpu_efficiency_metrics(timestamps, cpu_efficiency_values)
    
    print("Creating Memory Efficiency plot...")
    memory_efficiency_image_base64 = plot_memory_efficiency_metrics(timestamps, memory_efficiency_values)
    
    print("Generating markdown report...")
    markdown_content = create_markdown_report(rss_image_base64, cpu_image_base64, latency_image_base64, throughput_image_base64, msg_rate_image_base64, cpu_efficiency_image_base64, memory_efficiency_image_base64, timestamps, rss_max_values, cpu_max_values, latency_avg_values, latency_max_values, throughput_values, msg_rate_values, cpu_efficiency_values, memory_efficiency_values, measured_flags)
    
    with open(output_file, 'w') as f:
        f.write(markdown_content)
    
    print(f"Report saved to {output_file}")

if __name__ == '__main__':
    main()

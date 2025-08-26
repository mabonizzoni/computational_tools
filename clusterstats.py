#!/usr/bin/env python3
"""
clusterstats.py - PBS Pro Cluster Utilization Parser

Automatically calls pbsnodes and calculates cluster utilization statistics.
Tracks compute and GPU resources separately using queue-based detection.
Uses pbsnodes -av for detailed vnode information and reliable queue data.
"""

import json
import subprocess
import sys
from collections import defaultdict

# Color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

# Queue definitions
MONITORED_QUEUES = {
    'smallq', 'mediumq', 'largeq', 'expressq', 
    'bigmemq', 'commercialq', 'specialq', 'benchmarkq'
}

def parse_memory_value(mem_str):
    """Convert memory string to MB"""
    if not mem_str or mem_str == '<various>':
        return 0
    
    mem_str = str(mem_str).strip().lower()
    
    if mem_str.endswith('mb'):
        return float(mem_str[:-2])
    elif mem_str.endswith('kb'):
        return float(mem_str[:-2]) / 1024
    elif mem_str.endswith('gb'):
        return float(mem_str[:-2]) * 1024
    elif mem_str.endswith('b'):
        return float(mem_str[:-1]) / (1024 * 1024)
    else:
        # Assume bytes if no unit
        try:
            return float(mem_str) / (1024 * 1024)
        except ValueError:
            return 0

def safe_int_parse(value, default=0):
    """Safely parse integer values, handling <various> and other edge cases"""
    if not value or value == '<various>' or value == 'various':
        return default
    if isinstance(value, str) and value.strip() == '':
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def count_unique_jobs(jobs_str):
    """Count unique job IDs from jobs string"""
    if not jobs_str:
        return set()
    
    unique_jobs = set()
    job_entries = [job.strip() for job in jobs_str.split(',') if job.strip()]
    
    for job_entry in job_entries:
        # Extract job ID (everything before the first '/')
        if '/' in job_entry:
            job_id = job_entry.split('/')[0]
            unique_jobs.add(job_id)
        else:
            unique_jobs.add(job_entry)
    
    return unique_jobs

def should_include_node(node_name, state, qlist, vntype):
    """
    Determine if a node should be included in utilization calculations
    Returns: (should_include: bool, reason: str, node_type: str)
    """
    # First check: Skip nodes that are offline or down
    # Mixed states like 'job-busy,offline' should be treated as offline
    # since they're not available for new job scheduling
    state_str = str(state).upper()
    if 'DOWN' in state_str or 'OFFLINE' in state_str:
        return False, "down_offline", "unknown"
    
    # Second check: Only include actual compute/GPU nodes
    # Skip login nodes, storage nodes, etc. that might appear in pbsnodes
    if vntype not in ['compute_vnode', 'gpu_vnode']:
        return False, "not_compute_vnode", "unknown"
    
    # Third check: Queue-based categorization
    # This is our main logic for determining node purpose
    if qlist:
        node_queues = set(str(qlist).split(','))
        node_queues = {q.strip() for q in node_queues if q.strip()}
        
        # GPU nodes: Any node serving the GPU queue
        # These get separate resource tracking (CPU cores + GPU devices)
        if 'gpuq' in node_queues:
            return True, "included", "gpu"
        
        # Compute nodes: Any node serving our monitored batch queues
        # Include nodes even if they also serve other queues (e.g., "smallq,classq")
        if MONITORED_QUEUES & node_queues:
            return True, "included", "compute"
        
        # Exclude nodes that only serve non-monitored queues
        # These are dedicated interactive, class, or admin nodes
        return False, "no_monitored_queues", "compute"
    
    # Fallback: If no queue list, assume it's a compute node
    # This shouldn't happen in practice but provides safe default
    return True, "included", "compute"

def get_color_for_utilization(percentage):
    """Get color code based on utilization percentage"""
    if percentage < 50:
        return Colors.GREEN
    elif percentage <= 70:
        return Colors.YELLOW
    else:
        return Colors.RED

def calculate_utilization_display(assigned, available, resource_name):
    """Calculate utilization percentage and return formatted display string with color"""
    if available > 0:
        util_percent = (assigned / available) * 100
        color = get_color_for_utilization(util_percent)
        if resource_name == "memory":
            return f"{color}{format_memory(assigned)} used / {format_memory(available)} total ({util_percent:.1f}%){Colors.RESET}"
        else:  # cores or devices - format as integers
            return f"{color}{assigned:,} used / {available:,} total {resource_name} ({util_percent:.1f}%){Colors.RESET}"
    else:
        if resource_name == "memory":
            return f"0 used / 0 total (0.0%)"
        else:  # cores or devices
            return f"0 used / 0 total {resource_name} (0.0%)"

def format_memory(mb):
    """Format memory in human-readable units"""
    if mb >= 1024 * 1024:
        return f"{mb / (1024 * 1024):.1f} TB"
    elif mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    else:
        return f"{mb:.0f} MB"

def run_pbsnodes():
    """Execute pbsnodes command and return JSON data"""
    try:
        result = subprocess.run(
            ['pbsnodes', '-av', '-F', 'json'],
            capture_output=True,
            text=True,
            check=True
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running pbsnodes: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing pbsnodes JSON output: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: pbsnodes command not found. Make sure PBS Pro is installed and in PATH.")
        sys.exit(1)

def parse_pbsnodes_output(json_data):
    """Parse pbsnodes -av JSON output and return node information"""
    # Extract nodes from JSON structure
    nodes = json_data.get('nodes', {})
    processed_nodes = []
    
    for node_name, node_data in nodes.items():
        # Skip empty vnodes (vnodes with no actual resources)
        # These show up with 0 CPUs, 0 memory, 0 GPUs and don't contribute to utilization
        resources_available = node_data.get('resources_available', {})
        ncpus = safe_int_parse(resources_available.get('ncpus', 0))
        mem_mb = parse_memory_value(resources_available.get('mem', '0'))
        
        # Skip vnodes that have no computational resources
        if ncpus == 0 and mem_mb == 0:
            continue
        
        # Convert to our standard node format for processing
        processed_node = {
            'name': node_name,
            'state': ','.join(node_data.get('state', [])) if isinstance(node_data.get('state'), list) else str(node_data.get('state', 'unknown')),
            'resources_available.Qlist': resources_available.get('Qlist', ''),
            'resources_available.vntype': resources_available.get('vntype', ''),
            'resources_available.ncpus': str(ncpus),
            'resources_assigned.ncpus': str(safe_int_parse(node_data.get('resources_assigned', {}).get('ncpus', 0))),
            'resources_available.mem': resources_available.get('mem', '0'),
            'resources_assigned.mem': node_data.get('resources_assigned', {}).get('mem', '0'),
            'resources_available.ngpus': str(safe_int_parse(resources_available.get('ngpus', 0))),
            'resources_assigned.ngpus': str(safe_int_parse(node_data.get('resources_assigned', {}).get('ngpus', 0))),
            'jobs': ','.join(node_data.get('jobs', [])) if isinstance(node_data.get('jobs'), list) else str(node_data.get('jobs', ''))
        }
        
        processed_nodes.append(processed_node)
    
    return processed_nodes

def analyze_cluster():
    """Analyze cluster utilization and return statistics"""
    
    # Step 1: Get raw node data from PBS using JSON format with vnodes
    pbsnodes_data = run_pbsnodes()
    nodes = parse_pbsnodes_output(pbsnodes_data)
    
    # Step 2: Initialize statistics tracking
    # Separate tracking for compute vs GPU resources
    stats = {
        'compute': {
            'included_nodes': 0,
            'offline_nodes': 0,
            'cores_available': 0,
            'cores_assigned': 0,
            'memory_available_mb': 0,
            'memory_assigned_mb': 0,
            'unique_jobs': set()
        },
        'gpu': {
            'included_nodes': 0,
            'offline_nodes': 0,
            'cpu_cores_available': 0,    # CPU cores on GPU nodes
            'cpu_cores_assigned': 0,
            'gpu_devices_available': 0,  # Actual GPU devices (ngpus)
            'gpu_devices_assigned': 0,
            'memory_available_mb': 0,    # System memory on GPU nodes
            'memory_assigned_mb': 0,
            'unique_jobs': set()
        },
        'total_nodes': 0,
        'excluded_counts': defaultdict(int),    # Count by exclusion reason
        'excluded_nodes': defaultdict(list)     # Actual node names by reason
    }
    
    # Step 3: Process each vnode and categorize it
    # Note: We're now processing vnodes, but aggregating them transparently
    for node in nodes:
        stats['total_nodes'] += 1
        node_name = node['name']
        
        # Extract key node properties for decision making
        state = node.get('state', 'unknown')
        qlist = node.get('resources_available.Qlist', '')
        vntype = node.get('resources_available.vntype', '')
        
        # Step 4: Apply inclusion/exclusion logic
        include, reason, node_type = should_include_node(node_name, state, qlist, vntype)
        
        # Step 5: Handle offline vnodes - skip for simplified model
        # In the unified model, we don't track offline counts separately
        if reason == "down_offline":
            continue
        
        # Step 6: Handle excluded nodes
        # Track these for reporting what's not being counted
        if not include:
            stats['excluded_counts'][reason] += 1
            stats['excluded_nodes'][reason].append(node_name)
            continue
        
        # Step 7: Extract and aggregate resource information for included nodes
        # Use safe parsing to handle PBS edge cases like '<various>' values
        ncpus_available = safe_int_parse(node.get('resources_available.ncpus', 0))
        ncpus_assigned = safe_int_parse(node.get('resources_assigned.ncpus', 0))
        
        mem_available = parse_memory_value(node.get('resources_available.mem', '0'))
        mem_assigned = parse_memory_value(node.get('resources_assigned.mem', '0'))
        
        # Extract job information for utilization tracking
        jobs_str = node.get('jobs', '')
        unique_jobs = count_unique_jobs(jobs_str)
        
        # Step 8: Add resources to unified pools
        # All CPU cores and memory go into the main compute pool
        # GPU devices are tracked separately but jobs may overlap
        stats['compute']['cores_available'] += ncpus_available
        stats['compute']['cores_assigned'] += ncpus_assigned
        stats['compute']['memory_available_mb'] += mem_available
        stats['compute']['memory_assigned_mb'] += mem_assigned
        stats['compute']['unique_jobs'].update(unique_jobs)
        
        # If this is a GPU vnode, also track GPU devices
        if node_type == "gpu":
            ngpus_available = safe_int_parse(node.get('resources_available.ngpus', 0))
            ngpus_assigned = safe_int_parse(node.get('resources_assigned.ngpus', 0))
            stats['gpu']['gpu_devices_available'] += ngpus_available
            stats['gpu']['gpu_devices_assigned'] += ngpus_assigned
            stats['gpu']['unique_jobs'].update(unique_jobs)
    
    return stats

def print_utilization_report(stats):
    """Print formatted utilization report"""
    
    print(f"{Colors.BOLD}CLUSTER UTILIZATION{Colors.RESET}")
    print("=" * 19)
    
    # Section 1: Unified CPU and Memory (all nodes combined)
    print(f"CPU:          {calculate_utilization_display(stats['compute']['cores_assigned'], stats['compute']['cores_available'], 'cores')}")
    print(f"Memory:       {calculate_utilization_display(stats['compute']['memory_assigned_mb'], stats['compute']['memory_available_mb'], 'memory')}")
    
    # Section 2: GPU devices (only show if GPUs exist in cluster)  
    if stats['gpu']['gpu_devices_available'] > 0:
        print(f"GPUs:         {calculate_utilization_display(stats['gpu']['gpu_devices_assigned'], stats['gpu']['gpu_devices_available'], 'devices')}")
    
    # Section 3: Job summary
    print()
    total_jobs = len(stats['compute']['unique_jobs'])
    gpu_jobs = len(stats['gpu']['unique_jobs'])
    
    if gpu_jobs > 0:
        print(f"Jobs:         {total_jobs} running total, {gpu_jobs} using GPUs")
    else:
        print(f"Jobs:         {total_jobs} running")
    
    # Section 4: Exclusions summary (unchanged)
    exclusions = []
    
    if stats['excluded_counts']['no_monitored_queues'] > 0:
        excluded_nodes = ', '.join(sorted(stats['excluded_nodes']['no_monitored_queues']))
        plural = "nodes" if stats['excluded_counts']['no_monitored_queues'] > 1 else "node"
        exclusions.append(f"  {stats['excluded_counts']['no_monitored_queues']} {plural} with no monitored queues ({excluded_nodes})")
    
    if stats['excluded_counts']['not_compute_vnode'] > 0:
        other_nodes = ', '.join(sorted(stats['excluded_nodes']['not_compute_vnode']))
        plural = "nodes" if stats['excluded_counts']['not_compute_vnode'] > 1 else "node"
        exclusions.append(f"  {stats['excluded_counts']['not_compute_vnode']} non-compute {plural} ({other_nodes})")
    
    if exclusions:
        print(f"\nExcluded:")
        for exclusion in exclusions:
            print(exclusion)

def main():
    """Main function"""
    try:
        stats = analyze_cluster()
        print_utilization_report(stats)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
PBS Pro Cluster Utilization Parser

Automatically calls pbsnodes and calculates cluster utilization statistics.
Tracks compute and GPU resources separately.
"""

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
    if not mem_str:
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
    if not value or value == '<various>':
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

def is_gpu_node(node_name):
    """Check if node is a GPU node based on name pattern"""
    return 'g' in node_name.lower()

def should_include_node(node_name, state, qlist, vntype):
    """
    Determine if a node should be included in utilization calculations
    Returns: (should_include: bool, reason: str, node_type: str)
    """
    # Check if it's a GPU node
    if is_gpu_node(node_name):
        node_type = "gpu"
    else:
        node_type = "compute"
    
    # Skip if state contains DOWN or is offline
    state_str = str(state).upper()
    if 'DOWN' in state_str or 'OFFLINE' in state_str:
        return False, "down_offline", node_type
    
    # Only include compute_vnode and gpu_vnode types
    if vntype not in ['compute_vnode', 'gpu_vnode']:
        return False, "not_compute_vnode", node_type
    
    # Check queue assignments for non-GPU nodes
    if not is_gpu_node(node_name) and qlist:
        node_queues = set(str(qlist).split(','))
        node_queues = {q.strip() for q in node_queues if q.strip()}
        
        # Check for specific excluded queue types
        if 'interactiveq' in node_queues:
            return False, "interactive", node_type
        elif 'classq' in node_queues:
            return False, "class", node_type
        elif 'sysadminq' in node_queues and not (MONITORED_QUEUES & node_queues):
            return False, "admin", node_type
        
        # Include if node has at least one monitored queue
        if not (MONITORED_QUEUES & node_queues):
            return False, "other_excluded", node_type
    
    return True, "included", node_type

def get_color_for_utilization(percentage):
    """Get color code based on utilization percentage"""
    if percentage < 50:
        return Colors.GREEN
    elif percentage <= 70:
        return Colors.YELLOW
    else:
        return Colors.RED

def format_memory(mb):
    """Format memory in human-readable units"""
    if mb >= 1024 * 1024:
        return f"{mb / (1024 * 1024):.1f} TB"
    elif mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    else:
        return f"{mb:.0f} MB"

def run_pbsnodes():
    """Execute pbsnodes command and return text output"""
    try:
        result = subprocess.run(
            ['pbsnodes', '-a'],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running pbsnodes: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("Error: pbsnodes command not found. Make sure PBS Pro is installed and in PATH.")
        sys.exit(1)

def parse_pbsnodes_output(content):
    """Parse pbsnodes -a text output and return node information"""
    lines = content.strip().split('\n')
    nodes = []
    current_node = None
    
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
            
        if not line.startswith(' ') and not line.startswith('\t'):
            # New node name
            if current_node:
                nodes.append(current_node)
            current_node = {'name': line.strip()}
        else:
            # Attribute line
            if current_node is None:
                continue
            line = line.strip()
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                current_node[key] = value
    
    # Don't forget the last node
    if current_node:
        nodes.append(current_node)
    
    return nodes

def analyze_cluster():
    """Analyze cluster utilization and return statistics"""
    
    # Get node data
    pbsnodes_output = run_pbsnodes()
    nodes = parse_pbsnodes_output(pbsnodes_output)
    
    # Initialize counters
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
            'cpu_cores_available': 0,
            'cpu_cores_assigned': 0,
            'gpu_devices_available': 0,
            'gpu_devices_assigned': 0,
            'memory_available_mb': 0,
            'memory_assigned_mb': 0,
            'unique_jobs': set()
        },
        'total_nodes': 0,
        'excluded_counts': defaultdict(int),
        'excluded_nodes': defaultdict(list)
    }
    
    # Process each node
    for node in nodes:
        stats['total_nodes'] += 1
        node_name = node['name']
        
        # Extract node properties
        state = node.get('state', 'unknown')
        qlist = node.get('resources_available.Qlist', '')
        vntype = node.get('resources_available.vntype', '')
        
        # Determine if node should be included
        include, reason, node_type = should_include_node(node_name, state, qlist, vntype)
        
        # Handle offline nodes
        if reason == "down_offline":
            if node_type == "gpu":
                stats['gpu']['offline_nodes'] += 1
            else:
                stats['compute']['offline_nodes'] += 1
            continue
        
        # Handle excluded nodes
        if not include:
            stats['excluded_counts'][reason] += 1
            stats['excluded_nodes'][reason].append(node_name)
            continue
        
        # Extract resource information
        ncpus_available = safe_int_parse(node.get('resources_available.ncpus', 0))
        ncpus_assigned = safe_int_parse(node.get('resources_assigned.ncpus', 0))
        
        mem_available = parse_memory_value(node.get('resources_available.mem', '0'))
        mem_assigned = parse_memory_value(node.get('resources_assigned.mem', '0'))
        
        # Count unique jobs
        jobs_str = node.get('jobs', '')
        unique_jobs = count_unique_jobs(jobs_str)
        
        # Add to appropriate category
        if node_type == "gpu":
            stats['gpu']['included_nodes'] += 1
            stats['gpu']['cpu_cores_available'] += ncpus_available
            stats['gpu']['cpu_cores_assigned'] += ncpus_assigned
            stats['gpu']['memory_available_mb'] += mem_available
            stats['gpu']['memory_assigned_mb'] += mem_assigned
            stats['gpu']['unique_jobs'].update(unique_jobs)
            
            # Handle GPU devices
            ngpus_available = safe_int_parse(node.get('resources_available.ngpus', 0))
            ngpus_assigned = safe_int_parse(node.get('resources_assigned.ngpus', 0))
            stats['gpu']['gpu_devices_available'] += ngpus_available
            stats['gpu']['gpu_devices_assigned'] += ngpus_assigned
            
        else:  # compute node
            stats['compute']['included_nodes'] += 1
            stats['compute']['cores_available'] += ncpus_available
            stats['compute']['cores_assigned'] += ncpus_assigned
            stats['compute']['memory_available_mb'] += mem_available
            stats['compute']['memory_assigned_mb'] += mem_assigned
            stats['compute']['unique_jobs'].update(unique_jobs)
    
    return stats

def print_utilization_report(stats):
    """Print formatted utilization report"""
    
    print(f"{Colors.BOLD}PBS CLUSTER UTILIZATION{Colors.RESET}")
    print("=" * 24)
    
    # Compute section
    compute_total_nodes = stats['compute']['included_nodes'] + stats['compute']['offline_nodes']
    print(f"Compute Nodes: {stats['compute']['included_nodes']} active, {stats['compute']['offline_nodes']} offline ({compute_total_nodes} total)")
    
    if stats['compute']['cores_available'] > 0:
        cpu_util = (stats['compute']['cores_assigned'] / stats['compute']['cores_available']) * 100
        cpu_color = get_color_for_utilization(cpu_util)
        print(f"CPU:          {cpu_color}{stats['compute']['cores_assigned']:,} used / {stats['compute']['cores_available']:,} total cores ({cpu_util:.1f}%){Colors.RESET}")
    else:
        print(f"CPU:          0 used / 0 total cores (0.0%)")
    
    if stats['compute']['memory_available_mb'] > 0:
        mem_util = (stats['compute']['memory_assigned_mb'] / stats['compute']['memory_available_mb']) * 100
        mem_color = get_color_for_utilization(mem_util)
        print(f"Memory:       {mem_color}{format_memory(stats['compute']['memory_assigned_mb'])} used / {format_memory(stats['compute']['memory_available_mb'])} total ({mem_util:.1f}%){Colors.RESET}")
    else:
        print(f"Memory:       0 used / 0 total (0.0%)")
    
    # GPU section (only show if GPU nodes exist)
    if stats['gpu']['included_nodes'] > 0 or stats['gpu']['offline_nodes'] > 0:
        print()
        gpu_total_nodes = stats['gpu']['included_nodes'] + stats['gpu']['offline_nodes']
        print(f"GPU Nodes:    {stats['gpu']['included_nodes']} active, {stats['gpu']['offline_nodes']} offline ({gpu_total_nodes} total)")
        
        if stats['gpu']['cpu_cores_available'] > 0:
            gpu_cpu_util = (stats['gpu']['cpu_cores_assigned'] / stats['gpu']['cpu_cores_available']) * 100
            gpu_cpu_color = get_color_for_utilization(gpu_cpu_util)
            print(f"CPU:          {gpu_cpu_color}{stats['gpu']['cpu_cores_assigned']:,} used / {stats['gpu']['cpu_cores_available']:,} total cores ({gpu_cpu_util:.1f}%){Colors.RESET}")
        else:
            print(f"CPU:          0 used / 0 total cores (0.0%)")
        
        if stats['gpu']['gpu_devices_available'] > 0:
            gpu_util = (stats['gpu']['gpu_devices_assigned'] / stats['gpu']['gpu_devices_available']) * 100
            gpu_color = get_color_for_utilization(gpu_util)
            print(f"GPUs:         {gpu_color}{stats['gpu']['gpu_devices_assigned']:,} used / {stats['gpu']['gpu_devices_available']:,} total devices ({gpu_util:.1f}%){Colors.RESET}")
        else:
            print(f"GPUs:         0 used / 0 total devices (0.0%)")
        
        if stats['gpu']['memory_available_mb'] > 0:
            gpu_mem_util = (stats['gpu']['memory_assigned_mb'] / stats['gpu']['memory_available_mb']) * 100
            gpu_mem_color = get_color_for_utilization(gpu_mem_util)
            print(f"GPU Memory:   {gpu_mem_color}{format_memory(stats['gpu']['memory_assigned_mb'])} used / {format_memory(stats['gpu']['memory_available_mb'])} total ({gpu_mem_util:.1f}%){Colors.RESET}")
        else:
            print(f"GPU Memory:   0 used / 0 total (0.0%)")
    
    # Jobs section
    print()
    total_compute_jobs = len(stats['compute']['unique_jobs'])
    total_gpu_jobs = len(stats['gpu']['unique_jobs'])
    
    if total_gpu_jobs > 0:
        print(f"Jobs:         {total_compute_jobs} running (compute), {total_gpu_jobs} running (GPU)")
    else:
        print(f"Jobs:         {total_compute_jobs} running")
    
    # Print exclusions with node names
    exclusions = []
    
    # Interactive nodes
    if stats['excluded_counts']['interactive'] > 0:
        interactive_nodes = ', '.join(sorted(stats['excluded_nodes']['interactive']))
        plural = "nodes" if stats['excluded_counts']['interactive'] > 1 else "node"
        exclusions.append(f"  {stats['excluded_counts']['interactive']} interactive {plural} ({interactive_nodes})")
    
    # Admin nodes  
    if stats['excluded_counts']['admin'] > 0:
        admin_nodes = ', '.join(sorted(stats['excluded_nodes']['admin']))
        plural = "nodes" if stats['excluded_counts']['admin'] > 1 else "node"
        exclusions.append(f"  {stats['excluded_counts']['admin']} admin {plural} ({admin_nodes})")
    
    # Class nodes
    if stats['excluded_counts']['class'] > 0:
        class_nodes = ', '.join(sorted(stats['excluded_nodes']['class']))
        plural = "nodes" if stats['excluded_counts']['class'] > 1 else "node"
        exclusions.append(f"  {stats['excluded_counts']['class']} class {plural} ({class_nodes})")
    
    # Other excluded nodes
    if stats['excluded_counts']['other_excluded'] > 0:
        other_nodes = ', '.join(sorted(stats['excluded_nodes']['other_excluded']))
        plural = "nodes" if stats['excluded_counts']['other_excluded'] > 1 else "node"
        exclusions.append(f"  {stats['excluded_counts']['other_excluded']} other excluded {plural} ({other_nodes})")
    
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

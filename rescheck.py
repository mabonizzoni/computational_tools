#!/usr/bin/env python3
"""
HPC Cluster Resource Availability Checker

Checks if requested cores and memory are available on the cluster by calling pbsnodes
and parsing the output to find available resources on compute nodes. Automatically
determines eligible queues based on resource requirements.
"""

import argparse
import subprocess
import sys


def parse_memory_mb(mem_str):
    """Convert memory string to MB (e.g., '498gb' -> 498*1024, '152mb' -> 152)"""
    if not mem_str:
        return 0
        
    mem_str = mem_str.strip().lower()
    if not mem_str or mem_str == '0':
        return 0
    
    try:
        if mem_str.endswith('tb'):
            return int(float(mem_str[:-2]) * 1024 * 1024)
        elif mem_str.endswith('gb'):
            return int(float(mem_str[:-2]) * 1024)
        elif mem_str.endswith('mb'):
            return int(float(mem_str[:-2]))
        elif mem_str.endswith('kb'):
            return int(float(mem_str[:-2]) / 1024)
        elif mem_str.endswith('b'):
            return int(float(mem_str[:-1]) / (1024 * 1024))
        else:
            # Assume MB if no unit
            return int(float(mem_str))
    except (ValueError, IndexError):
        return 0


def mb_to_gb(mb):
    """Convert MB to GB as integer"""
    return int(mb // 1024)


def determine_eligible_queues(cores, memory_gb):
    """Determine which queues are eligible for the given resource request"""
    eligible = []
    
    # Check each queue's limits (order doesn't matter here)
    if cores <= 4 and memory_gb <= 16:
        eligible.append('expressq')
    if cores <= 8 and memory_gb <= 4:
        eligible.append('smallq')
    if cores <= 16 and memory_gb <= 16:
        eligible.append('mediumq')
    if cores <= 128 and memory_gb <= 120:
        eligible.append('largeq')
    # bigmemq has both minimum AND maximum memory requirements
    if cores <= 32 and 130 <= memory_gb <= 500:
        eligible.append('bigmemq')
    
    return eligible


def get_preferred_queue(eligible_queues):
    """Get the preferred regular queue and check if express is available"""
    # Regular queue priorities (smallest suitable preferred)
    regular_queue_priority = {
        'smallq': 4,    # highest priority - least competition
        'mediumq': 3,   # still less competition than large  
        'largeq': 2,    # more competition but necessary for big jobs
        'bigmemq': 1    # lowest priority - only when high memory needed
    }
    
    # Find best regular queue
    regular_queues = [q for q in eligible_queues if q in regular_queue_priority]
    preferred_regular = None
    if regular_queues:
        preferred_regular = max(regular_queues, key=lambda x: regular_queue_priority[x])
    
    # Check if express is eligible
    express_eligible = 'expressq' in eligible_queues
    
    return preferred_regular, express_eligible


def node_supports_eligible_queue(node_qlist, eligible_queues):
    """Check if node supports any of the eligible queues"""
    if not eligible_queues:
        return False
    
    node_queues = [q.strip() for q in node_qlist.split(',')]
    return any(eq in node_queues for eq in eligible_queues)


def should_include_node(node, eligible_queues):
    """Check if node should be included based on state, type, and queue support"""
    state = node.get('state', '').lower()
    vntype = node.get('resources_available.vntype', '')
    qlist = node.get('resources_available.Qlist', '')
    
    # Skip offline or unknown state nodes (handle comma-separated states)
    excluded_states = ['offline', 'unknown', 'down']
    if any(excluded_state in state for excluded_state in excluded_states):
        return False
    
    # Only include compute nodes
    if vntype != 'compute_vnode':
        return False
    
    # Check if node supports any eligible queue
    if not node_supports_eligible_queue(qlist, eligible_queues):
        return False
        
    return True


def parse_pbsnodes_output(output, eligible_queues):
    """Parse pbsnodes -a output and return list of node information"""
    lines = output.strip().split('\n')
    nodes = []
    current_node = None
    
    for line in lines:
        line = line.rstrip()
        if not line:
            continue
            
        if not line.startswith(' ') and not line.startswith('\t'):
            # New node name
            if current_node and should_include_node(current_node, eligible_queues):
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
    if current_node and should_include_node(current_node, eligible_queues):
        nodes.append(current_node)
    
    # Convert to our standard format
    processed_nodes = []
    for node in nodes:
        processed_node = process_node(node)
        if processed_node:
            processed_nodes.append(processed_node)
    
    return processed_nodes


def process_node(node):
    """Process a node dict into our standard format"""
    try:
        name = node['name']
        state = node.get('state', 'unknown')
        
        # Get total resources
        total_mem_mb = parse_memory_mb(node.get('resources_available.mem', '0mb'))
        total_cpus = int(node.get('resources_available.ncpus', '0'))
        
        # Get assigned resources - note that assigned memory is in KB!
        assigned_mem_mb = parse_memory_mb(node.get('resources_assigned.mem', '0kb'))
        assigned_cpus = int(node.get('resources_assigned.ncpus', '0'))
        
        # Calculate available resources
        available_mem_mb = total_mem_mb - assigned_mem_mb
        available_cpus = total_cpus - assigned_cpus
        
        # Ensure non-negative values
        available_mem_mb = max(0, available_mem_mb)
        available_cpus = max(0, available_cpus)
        
        return {
            'name': name,
            'state': state,
            'cpu_available': available_cpus,
            'cpu_total': total_cpus,
            'mem_available_mb': available_mem_mb,
            'mem_total_mb': total_mem_mb
        }
    except (ValueError, KeyError) as e:
        # Skip nodes with parsing errors
        return None


def find_available_nodes(nodes, required_cores, required_memory_mb):
    """Find nodes that can accommodate the resource request"""
    available = []
    
    for node in nodes:
        if (node['cpu_available'] >= required_cores and 
            node['mem_available_mb'] >= required_memory_mb):
            available.append(node)
    
    return available


def get_best_alternatives(nodes, required_cores, required_memory_mb, max_options=5):
    """Get best alternative options when exact requirements can't be met"""
    # Filter nodes that have at least some resources
    viable_nodes = [node for node in nodes if node['cpu_available'] > 0 and node['mem_available_mb'] > 0]
    
    # Sort by available cores (descending), then by available memory (descending)
    viable_nodes.sort(key=lambda x: (x['cpu_available'], x['mem_available_mb']), reverse=True)
    
    # Return 2-5 options if available, otherwise return what we have
    if len(viable_nodes) <= 1:
        return viable_nodes
    else:
        return viable_nodes[:min(max_options, len(viable_nodes))]


# Parse command line arguments
parser = argparse.ArgumentParser(description='Check HPC cluster resource availability')
parser.add_argument('cores', type=int, help='Number of cores required')
parser.add_argument('memory', type=float, nargs='?', help='Memory required in GB (optional)')
parser.add_argument('--mem-per-core', type=float, default=1.5, 
                    help='Memory per core in GB (default: 1.5)')
parser.add_argument('--mem-overhead', type=float, default=4.0,
                    help='Memory overhead in GB (default: 4.0)')

args = parser.parse_args()

# Calculate memory requirement
if args.memory is not None:
    required_memory_gb = args.memory
else:
    required_memory_gb = args.cores * args.mem_per_core + args.mem_overhead

required_memory_mb = int(required_memory_gb * 1024)

# Determine eligible queues based on resource request
eligible_queues = determine_eligible_queues(args.cores, required_memory_gb)
preferred_regular, express_eligible = get_preferred_queue(eligible_queues)

if not eligible_queues:
    print(f"Error: Resource request ({args.cores} cores, {int(required_memory_gb)} GB) exceeds all queue limits")
    sys.exit(1)

print(f"Looking for {args.cores} cores and {int(required_memory_gb)} GB of memory")

# Show queue recommendations
if preferred_regular and express_eligible:
    regular_name = preferred_regular.replace('q', '')
    print(f"Recommended queues: {regular_name} or express (4hr limit)")
elif preferred_regular:
    regular_name = preferred_regular.replace('q', '')
    print(f"Recommended queue: {regular_name}")
elif express_eligible:
    print(f"Recommended queue: express (4hr limit)")

# Run pbsnodes
try:
    result = subprocess.run(['pbsnodes', '-a'], capture_output=True, text=True, check=True)
    pbsnodes_output = result.stdout
except subprocess.CalledProcessError as e:
    print(f"Error running pbsnodes: {e}", file=sys.stderr)
    sys.exit(1)
except FileNotFoundError:
    print("Error: pbsnodes command not found", file=sys.stderr)
    sys.exit(1)

# Parse output
nodes = parse_pbsnodes_output(pbsnodes_output, eligible_queues)

if not nodes:
    print(f"No compute nodes found supporting eligible queues: {', '.join(q.replace('q', '') for q in eligible_queues)}")
    sys.exit(1)

# Check for available resources
available_nodes = find_available_nodes(nodes, args.cores, required_memory_mb)

if available_nodes:
    print("Yes")
    # Sort by available cores (descending)
    available_nodes.sort(key=lambda x: x['cpu_available'], reverse=True)
    for node in available_nodes:
        print(f"• {node['name']}: {node['cpu_available']} cores available, "
              f"{mb_to_gb(node['mem_available_mb'])} GB available")
    sys.exit(0)
else:
    print("No, largest available allocations:")
    alternatives = get_best_alternatives(nodes, args.cores, required_memory_mb)
    for node in alternatives:
        print(f"• {node['name']}: {node['cpu_available']} cores available, "
              f"{mb_to_gb(node['mem_available_mb'])} GB available")
    sys.exit(1)
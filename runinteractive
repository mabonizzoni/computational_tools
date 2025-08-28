#!/bin/bash

# Defaults will be set after querying queue limits
default_cores=""
default_memory=""
default_walltime=""

# Queue limits (will be populated dynamically)
max_cores=""
max_memory=""
max_walltime=""
max_jobs_per_user=""

# Interactive node (will be populated dynamically)
interactive_node=""

get_interactive_node() {
    # Query PBS Pro for the interactive queue node
    local nodes=$(pbsnodes -av -F json 2>/dev/null | jq -r '.nodes | to_entries[] | select(.value | tostring | contains("interactive")) | .key' 2>/dev/null)
    
    if [ -z "$nodes" ]; then
        # Fallback to asax039 if detection fails
        echo "WARNING: Could not detect interactive node, using fallback 'asax039'" >&2
        interactive_node="asax039"
        return 1
    else
        interactive_node="$nodes"
        return 0
    fi
}

get_queue_limits() {
    # Query PBS Pro for current interactive queue limits
    local queue_info=$(qstat -Qf interactive -F json 2>/dev/null)
    
    if [ $? -ne 0 ] || [ -z "$queue_info" ]; then
        # Fallback to reasonable defaults if query fails
        echo "WARNING: Could not query interactive queue limits, using fallback defaults" >&2
        max_cores=4
        max_memory="8gb" 
        max_walltime="8:00:00"
        max_jobs_per_user=1
        return 1
    fi
    
    # Extract limits using jq with fallbacks
    max_cores=$(echo "$queue_info" | jq -r '.Queue.interactive.resources_max.ncpus // 4')
    max_memory=$(echo "$queue_info" | jq -r '.Queue.interactive.resources_max.mem // "8gb"')
    max_walltime=$(echo "$queue_info" | jq -r '.Queue.interactive.resources_max.walltime // "8:00:00"')
    
    # Extract max_run - format is typically "[u:PBS_GENERIC=1]"
    local max_run_raw=$(echo "$queue_info" | jq -r '.Queue.interactive.max_run // "1"')
    max_jobs_per_user=$(echo "$max_run_raw" | grep -o '[0-9]*' | head -1)
    max_jobs_per_user=${max_jobs_per_user:-1}
    
    # Set defaults to queue maximums
    default_cores=$max_cores
    default_memory=$max_memory
    default_walltime=$max_walltime
    
    return 0
}

show_usage() {
    # Ensure limits are loaded
    if [ -z "$max_cores" ]; then
        get_queue_limits > /dev/null 2>&1
    fi
    
    # Ensure interactive node is identified
    if [ -z "$interactive_node" ]; then
        get_interactive_node > /dev/null 2>&1
    fi
    
    echo "Usage: runinteractive [OPTIONS] [cores] [memory] [walltime]"
    echo ""
    echo "OPTIONS:"
    echo "  -h, --help     Show this help message"
    echo "  --status       Show interactive node status and exit"
    echo "  --force        Submit job without confirmation or availability check"
    echo ""
    echo "EXAMPLES:"
    echo "  runinteractive"
    echo "  runinteractive 2"
    echo "  runinteractive 2 4gb"
    echo "  runinteractive 2 4gb 2:00:00"
    echo "  runinteractive --force 4 8gb 8:00:00"
    echo ""
    echo "DEFAULTS: ${default_cores:-$max_cores} cores, ${default_memory:-$max_memory} memory, ${default_walltime:-$max_walltime} walltime"
    echo "LIMITS: Max $max_cores cores, $max_memory memory, $max_walltime walltime, $max_jobs_per_user job per user"
    echo "NOTE: All interactive jobs run on $interactive_node"
}

check_availability() {
    # Check if user already has an interactive job (this would block immediately)
    if qstat -u $USER 2>/dev/null | grep -q interactive; then
        echo "WARNING: You already have an interactive job running (limit: 1 per user)"
        return 2  # Special return code for existing job
    fi
    
    # Get detailed info from interactive node
    local node_info=$(pbsnodes $interactive_node 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo "WARNING: Cannot check node $interactive_node status - submitting anyway"
        return 1
    fi
    
    # Extract key information with better parsing
    local state=$(echo "$node_info" | grep "state = " | cut -d= -f2 | tr -d ' ')
    local total_cores=$(echo "$node_info" | grep "resources_available.ncpus" | cut -d= -f2 | tr -d ' ')
    local assigned_cores=$(echo "$node_info" | grep "resources_assigned.ncpus" | cut -d= -f2 | tr -d ' ')
    local total_mem=$(echo "$node_info" | grep "resources_available.mem" | cut -d= -f2 | tr -d ' ')
    local assigned_mem=$(echo "$node_info" | grep "resources_assigned.mem" | cut -d= -f2 | tr -d ' ')
    local jobs=$(echo "$node_info" | grep "jobs = " | cut -d= -f2)
    
    # Set defaults for empty values
    assigned_cores=${assigned_cores:-0}
    assigned_mem=${assigned_mem:-0}
    
    # Parse memory values more carefully
    local total_mem_num=$(echo "$total_mem" | grep -o '[0-9]*')
    local assigned_mem_num=$(echo "$assigned_mem" | grep -o '[0-9]*')
    
    # Set defaults if parsing failed
    total_mem_num=${total_mem_num:-0}
    assigned_mem_num=${assigned_mem_num:-0}
    
    # Convert to GB (assume input is in kb if it has 'kb', mb if 'mb', gb if 'gb')
    local total_mem_gb=0
    local assigned_mem_gb=0
    
    if [[ "$total_mem" == *"kb" ]]; then
        total_mem_gb=$((total_mem_num / 1024 / 1024))
    elif [[ "$total_mem" == *"mb" ]]; then
        total_mem_gb=$((total_mem_num / 1024))
    elif [[ "$total_mem" == *"gb" ]]; then
        total_mem_gb=$total_mem_num
    else
        total_mem_gb=0
    fi
    
    if [[ "$assigned_mem" == *"kb" ]]; then
        assigned_mem_gb=$((assigned_mem_num / 1024 / 1024))
    elif [[ "$assigned_mem" == *"mb" ]]; then
        assigned_mem_gb=$((assigned_mem_num / 1024))
    elif [[ "$assigned_mem" == *"gb" ]]; then
        assigned_mem_gb=$assigned_mem_num
    else
        assigned_mem_gb=0
    fi
    
    # Calculate free resources
    local free_cores=$((total_cores - assigned_cores))
    local free_mem_gb=$((total_mem_gb - assigned_mem_gb))
    
    # Display detailed status
    echo "Interactive Node ($interactive_node) Status:"
    echo "   State: $state"
    echo "   Cores: $free_cores/$total_cores free"
    if [ "$total_mem_gb" -eq 0 ]; then
        echo "   Memory: UNKNOWN"
    else
        echo "   Memory: ${free_mem_gb}GB/${total_mem_gb}GB free"
    fi
    
    if [[ -n "$jobs" && "$jobs" != " " ]]; then
        local job_count=$(echo "$jobs" | tr ',' '\n' | wc -l)
        echo "   Active jobs: $job_count"
    else
        echo "   Active jobs: 0"
    fi
    
    # Determine availability
    if [[ "$state" == "free" && -z "$jobs" ]] || [[ "$state" == "free" && "$jobs" == " " ]]; then
        echo "AVAILABLE: Node is completely free - should start immediately"
        return 0
    elif [[ "$state" == "free" && "$free_cores" -ge 4 ]]; then
        echo "AVAILABLE: Node has shared jobs but enough cores free - should start immediately"
        return 0
    elif [[ "$state" == *"job-exclusive"* ]]; then
        echo "BUSY: Node is running exclusive jobs - will queue"
        return 1
    elif [[ "$state" == *"down"* ]] || [[ "$state" == *"offline"* ]]; then
        echo "DOWN: Node is down/offline - will queue"
        return 1
    else
        echo "UNCERTAIN: Node status unclear or busy - will likely queue"
        return 1
    fi
}

# Parse command line arguments
force_mode=false
positional_args=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        --status)
            get_interactive_node
            check_availability
            exit 0
            ;;
        --force)
            force_mode=true
            shift
            ;;
        -*)
            echo "Unknown option $1"
            show_usage
            exit 1
            ;;
        *)
            positional_args+=("$1")
            shift
            ;;
    esac
done

# Check if too many positional arguments
if [ ${#positional_args[@]} -gt 3 ]; then
    echo "Error: Too many arguments"
    echo ""
    show_usage
    exit 1
fi

# Get current queue limits
get_queue_limits

# Get interactive node
get_interactive_node

# Parse positional arguments with defaults
cores=${positional_args[0]:-$default_cores}
memory=${positional_args[1]:-$default_memory}
walltime=${positional_args[2]:-$default_walltime}

# Extract numeric value from memory for validation
mem_value=$(echo "$memory" | sed 's/gb$//')
max_mem_value=$(echo "$max_memory" | sed 's/gb$//')

# Validate cores
if [ "$cores" -gt "$max_cores" ]; then
    echo "Error: Requested $cores cores exceeds interactive queue limit of $max_cores"
    exit 1
fi

# Validate memory
if [[ "$memory" == *gb ]] && [ "$mem_value" -gt "$max_mem_value" ]; then
    echo "Error: Requested ${memory} exceeds interactive queue limit of $max_memory"
    exit 1
fi

# Validate walltime (convert to seconds for comparison)
walltime_seconds=$(echo "$walltime" | awk -F: '{print ($1 * 3600) + ($2 * 60) + $3}')
max_walltime_seconds=$(echo "$max_walltime" | awk -F: '{print ($1 * 3600) + ($2 * 60) + $3}')

if [ "$walltime_seconds" -gt "$max_walltime_seconds" ]; then
    echo "Error: Requested walltime ${walltime} exceeds interactive queue limit of $max_walltime"
    exit 1
fi

# Check availability (unless force mode)
if [ "$force_mode" = false ]; then
    check_availability
    availability_status=$?
    
    echo ""
    
    if [ $availability_status -eq 2 ]; then
        # Already have a job running
        exit 1
    elif [ $availability_status -ne 0 ]; then
        # Resources not available - ask user if they want to queue
        read -p "Submit anyway and wait in queue? (y/N): " submit_anyway
        if [[ ! "$submit_anyway" =~ ^[Yy]$ ]]; then
            echo "Job submission cancelled."
            exit 0
        fi
        echo ""
    fi
fi

# Show job parameters and ask for confirmation (unless force mode)
if [ "$force_mode" = false ]; then
    echo "About to submit job with:"
    echo "  Cores: $cores"
    echo "  Memory: $memory"
    echo "  Walltime: $walltime"
    echo ""
    read -p "Continue? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "Job submission cancelled."
        exit 0
    fi
    echo ""
fi

# Submit the job
if [ "$force_mode" = true ]; then
    echo "Submitting job (${cores} cores, ${memory} memory, ${walltime} walltime)"
else
    echo "Submitting job..."
fi

qsub -I -q interactive -l select=1:ncpus=${cores}:mem=${memory} -l walltime=${walltime}

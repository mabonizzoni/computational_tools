#!/bin/bash

# Set defaults to queue maximums
default_cores=4
default_memory="8gb"
default_walltime="8:00:00"

check_availability() {
    # Check if user already has an interactive job (this would block immediately)
    if qstat -u $USER 2>/dev/null | grep -q interactive; then
        echo "❌ You already have an interactive job running (limit: 1 per user)"
        return 2  # Special return code for existing job
    fi
    
    # Get detailed info from asax039
    local node_info=$(pbsnodes asax039 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo "⚠️  Cannot check node asax039 status - submitting anyway"
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
        total_mem_gb="?"
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
    echo "📊 Interactive Node (asax039) Status:"
    echo "   State: $state"
    echo "   Cores: $free_cores/$total_cores free"
    echo "   Memory: ${free_mem_gb}GB/${total_mem_gb}GB free"
    
    if [[ -n "$jobs" && "$jobs" != " " ]]; then
        local job_count=$(echo "$jobs" | tr ',' '\n' | wc -l)
        echo "   Active jobs: $job_count"
    else
        echo "   Active jobs: 0"
    fi
    
    # Determine availability
    if [[ "$state" == "free" && -z "$jobs" ]] || [[ "$state" == "free" && "$jobs" == " " ]]; then
        echo "✅ Node is completely free - should start immediately"
        return 0
    elif [[ "$state" == "free" && "$free_cores" -ge 4 ]]; then
        echo "✅ Node has shared jobs but enough cores free - should start immediately"
        return 0
    elif [[ "$state" == *"job-exclusive"* ]]; then
        echo "❌ Node is running exclusive jobs - will queue"
        return 1
    elif [[ "$state" == *"down"* ]] || [[ "$state" == *"offline"* ]]; then
        echo "❌ Node is down/offline - will queue"
        return 1
    else
        echo "⚠️  Node status unclear or busy - will likely queue"
        return 1
    fi
}

# Rest of the script remains the same...
if [ $# -gt 3 ]; then
    echo "Usage: runinteractive [cores] [memory] [walltime]"
    echo "Example: runinteractive"
    echo "Example: runinteractive 2"
    echo "Example: runinteractive 2 4gb"
    echo "Example: runinteractive 2 4gb 2:00:00"
    echo ""
    echo "Defaults: ${default_cores} cores, ${default_memory} memory, ${default_walltime} walltime"
    echo "Interactive queue limits: Max 4 cores, 8gb memory, 8hrs, 1 job per user"
    echo "Note: All interactive jobs run on asax039"
    exit 1
fi

# Parse arguments with defaults
cores=${1:-$default_cores}
memory=${2:-$default_memory}
walltime=${3:-$default_walltime}

# Extract numeric value from memory for validation
mem_value=$(echo "$memory" | sed 's/gb$//')

# Validate cores
if [ "$cores" -gt 4 ]; then
    echo "Error: Requested $cores cores exceeds interactive queue limit of 4"
    exit 1
fi

# Validate memory
if [[ "$memory" == *gb ]] && [ "$mem_value" -gt 8 ]; then
    echo "Error: Requested ${memory} exceeds interactive queue limit of 8gb"
    exit 1
fi

# Check availability before submitting
check_availability
availability_status=$?

echo ""

if [ $availability_status -eq 2 ]; then
    # Already have a job running
    exit 1
elif [ $availability_status -eq 0 ]; then
    # Resources available - submit immediately
    echo "Submitting job (${cores} cores, ${memory} memory, ${walltime} walltime)"
    echo ""
else
    # Resources not available - ask user
    read -p "Submit anyway and wait in queue? (y/N): " submit_anyway
    if [[ ! "$submit_anyway" =~ ^[Yy]$ ]]; then
        echo "Job submission cancelled."
        exit 0
    fi
    echo ""
    echo "Submitting job to queue (${cores} cores, ${memory} memory, ${walltime} walltime)"
    echo "This will block until asax039 becomes available. Use Ctrl+C to cancel."
    echo ""
fi

qsub -I -q interactive -l select=1:ncpus=${cores}:mem=${memory} -l walltime=${walltime}
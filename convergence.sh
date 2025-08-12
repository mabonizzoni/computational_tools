#!/bin/bash

# Check if filename provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <gaussian_logfile>"
    echo "Example: $0 tddft_opt.log"
    exit 1
fi

# Extract convergence data and group properly by optimization step
grep -E "(Maximum Force|RMS.*Force|Maximum Displacement|RMS.*Displacement|SCF Done|Excited State[ ]+1:)" "$1" | awk '
BEGIN { step = 0; in_step = 0 }
/SCF Done/ { 
    if (in_step) print ""  # Add blank line before new step (except first)
    step++
    printf "=== Optimization Step %d ===\n", step
    print
    in_step = 1
    next
}
in_step { print }
'

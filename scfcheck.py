#!/usr/bin/env python3

import sys

# Read SCF Done lines from file (filename guaranteed to be valid by shell script)
data = []
line_num = 1
with open(sys.argv[1]) as f:
    for line in f:
        if "SCF Done" in line:
            # Extract energy value after "="
            energy = float(line.split('=')[1].split()[0])
            data.append((str(line_num), energy))
            line_num += 1

# Check if we found any SCF Done lines
if not data:
    print("No 'SCF Done' lines found in file")
    sys.exit(1)

# Find minimum and maximum, then normalize
min_val = min(value for _, value in data)
max_val = max(value for _, value in data)
range_val = max_val - min_val

for label, value in data:
    if range_val == 0:
        normalized = 0  # All values are the same
    else:
        normalized = (value - min_val) / range_val * 100
    
    bar_length = int(normalized)
    bar = '#' * bar_length
    print(f"{label:10} | {bar} ({value:.6f})")
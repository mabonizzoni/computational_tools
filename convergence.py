#!/usr/bin/env python3

import sys
import os
import subprocess
import json
import re

def find_running_job_logs():
    """Find .log files using PBS tracking files for precise filename detection"""
    try:
        # Get PBS job details in JSON format
        result = subprocess.run(['qstat', '-f', '-F', 'json'], capture_output=True, text=True)
        data = json.loads(result.stdout)

        log_files = []
        for job_id, job_info in data['Jobs'].items():
            job_name = job_info['Job_Name']
            workdir = job_info['Variable_List']['PBS_O_WORKDIR']

            # Extract numeric job ID (e.g., "318628" from "318628.asax-pbs1")
            numeric_id = job_id.split('.')[0]

            # Read PBS tracking file to get actual input filename
            tracking_file = os.path.join(workdir, f"{job_name}.i{numeric_id}")
            if os.path.exists(tracking_file):
                try:
                    with open(tracking_file) as f:
                        for line in f:
                            if "The submitted file is:" in line:
                                input_file = line.split(":")[1].strip()
                                log_file = input_file + ".log"  # Append .log to input filename
                                full_log_path = os.path.join(workdir, log_file)
                                if os.path.exists(full_log_path):
                                    log_files.append(full_log_path)
                                break
                except IOError:
                    # If tracking file can't be read, skip this job
                    continue

        return log_files

    except subprocess.CalledProcessError:
        print("Error: Could not run qstat command")
        return []
    except json.JSONDecodeError:
        print("Error: Could not parse qstat JSON output")
        return []
    except Exception as e:
        print(f"Error finding job logs: {e}")
        return []

def extract_convergence_data(filename):
    """Extract convergence data from Gaussian log file and group by optimization steps"""
    
    # Pattern to match all convergence-related lines
    convergence_pattern = re.compile(
        r'(Maximum Force|RMS.*Force|Maximum Displacement|RMS.*Displacement|SCF Done|Excited State[ ]+1:)'
    )
    
    steps = {}
    current_step = 0
    
    try:
        with open(filename, 'r') as f:
            for line in f:
                if convergence_pattern.search(line):
                    line = line.strip()
                    
                    # SCF Done marks the start of a new optimization step
                    if 'SCF Done' in line:
                        current_step += 1
                        steps[current_step] = [line]
                    elif current_step > 0:  # Add other convergence lines to current step
                        steps[current_step].append(line)
    
    except IOError as e:
        print(f"Error reading file {filename}: {e}")
        return {}
    
    return steps

def format_and_print_results(filename, steps):
    """Format and print convergence data in the same style as the bash script"""
    
    if not steps:
        print(f"No convergence data found in {filename}")
        return
    
    print(f"\nAnalyzing: {os.path.basename(filename)}")
    print("=" * 50)
    
    for step_num in sorted(steps.keys()):
        print(f"=== Optimization Step {step_num} ===")
        for line in steps[step_num]:
            print(line)
        if step_num < max(steps.keys()):  # Add blank line except after last step
            print()

# Handle command line arguments
if len(sys.argv) == 1:
    # No arguments - auto-discover from running PBS jobs
    print("No files specified - checking running PBS jobs...")
    files_to_process = find_running_job_logs()

    if not files_to_process:
        print("No log files found in running job directories")
        print("Usage: python3 convergence_analysis.py <logfile1> [logfile2] ...")
        sys.exit(1)

    print(f"Found {len(files_to_process)} log files from running jobs:")
    for f in files_to_process:
        print(f"  {f}")
    print()

else:
    # Files specified on command line
    files_to_process = sys.argv[1:]

# Process each file
for i, filename in enumerate(files_to_process):
    # Check file accessibility
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' not found - skipping")
        continue
    
    if not os.access(filename, os.R_OK):
        print(f"Error: File '{filename}' is not readable - skipping")
        continue
    
    # Extract and display convergence data
    convergence_steps = extract_convergence_data(filename)
    format_and_print_results(filename, convergence_steps)
    
    # Pause between files (except after the last one)
    if len(files_to_process) > 1 and i < len(files_to_process) - 1 and sys.stdout.isatty():
        next_file = files_to_process[i + 1]
        print(f"\nNext file: {os.path.basename(next_file)}")
        input("Press Enter to continue...")
        print()

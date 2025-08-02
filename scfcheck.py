#!/apps/x86-64/apps/anaconda_3-2024.10/bin/python

import sys
import os
import subprocess
import json

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

# Determine which files to process
if len(sys.argv) == 1:
    # No arguments provided - auto-discover from running jobs
    print("No files specified - checking running PBS jobs...")
    files_to_process = find_running_job_logs()

    if not files_to_process:
        print("No log files found in running job directories")
        sys.exit(1)

    print(f"Found {len(files_to_process)} log files from running jobs:")
    for f in files_to_process:
        print(f"  {f}")
    print()

else:
    # Files specified on command line
    files_to_process = sys.argv[1:]

# Add user site-packages to path (in case plotext is installed there)
user_site = os.path.expanduser('~/.local/lib/python3.12/site-packages')
if user_site not in sys.path:
    sys.path.insert(0, user_site)

import plotext as plt

# Process each file
for filename in files_to_process:
    print(f"\nProcessing: {filename}")

    # Check if file exists
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' not found - skipping")
        continue

    # Check if file is readable
    if not os.access(filename, os.R_OK):
        print(f"Error: File '{filename}' is not readable - skipping")
        continue

    # Read SCF Done lines from file
    iterations = []
    energies = []
    line_num = 1

    with open(filename) as f:
        for line in f:
            if "SCF Done" in line:
                # Extract energy value after "="
                energy = float(line.split('=')[1].split()[0])
                iterations.append(line_num)
                energies.append(energy)
                line_num += 1

    # Check if we found any SCF Done lines
    if not energies:
        print(f"No 'SCF Done' lines found in {filename} - skipping")
        continue

    # Convert to relative energies (lowest = 0) and kcal/mol
    min_energy = min(energies)
    energies_relative_kcal = [(e - min_energy) * 627.5094737775374 for e in energies]

    # Skip plotting if only one point
    if len(energies) == 1:
        print(f"Only one SCF point found in {os.path.basename(filename)} - no convergence to plot")
        continue

    # Plot with vertical padding and integer y ticks
    data_max = max(energies_relative_kcal)
    padding = data_max * 0.05

    # Create scatter plot
    plt.clear_data()  # Clear previous plot
    plt.scatter(iterations, energies_relative_kcal)
    plt.title(f"SCF Energy - {os.path.basename(filename)}")
    plt.xlabel("Iteration")
    plt.ylabel("Energy (kcal/mol)")

    # Smart automatic x-axis tick spacing
    total_iters = len(energies_relative_kcal)
    if total_iters <= 10:
        spacing = 1
    elif total_iters <= 50:
        spacing = 5
    elif total_iters <= 200:
        spacing = 10
    else:
        spacing = 50

    plt.xticks(range(0, total_iters+1, spacing))
    plt.ylim(-padding, data_max + padding)
    plt.yticks(range(0, int(data_max) + 1, 1))
    plt.show()

    # Brief pause between plots (optional)
    input("Press Enter for next file...")

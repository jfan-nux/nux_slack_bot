#!/bin/bash

# Experiment Analysis Pipeline Runner
# Runs experiment analysis and creates combined metrics table

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ✓${NC} $1"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠${NC} $1"
}

# Main execution
main() {
    print_status "Starting Experiment Analysis Pipeline..."
    echo "=================================================="
    
    # Step 1: Run experiments analysis
    print_status "Step 1: Running experiment analysis (run_experiments.py)"
    if python3 run_experiments.py; then
        print_success "Experiment analysis completed successfully"
    else
        print_error "Experiment analysis failed"
        exit 1
    fi
    
    echo ""
    
    # Step 2: Create combined metrics table  
    print_status "Step 2: Creating combined metrics table (create_combined_metrics_table.py)"
    if python3 create_combined_metrics_table.py; then
        print_success "Combined metrics table created successfully"
    else
        print_error "Combined metrics table creation failed"
        exit 1
    fi
    
    echo ""
    print_success "Pipeline completed successfully! 🎉"
    echo "=================================================="
    print_status "Results available in combined_experiment_metrics table"
}

# Check if Python scripts exist
if [[ ! -f "run_experiments.py" ]]; then
    print_error "run_experiments.py not found in current directory"
    exit 1
fi

if [[ ! -f "create_combined_metrics_table.py" ]]; then
    print_error "create_combined_metrics_table.py not found in current directory"
    exit 1
fi

# Run main function
main "$@"

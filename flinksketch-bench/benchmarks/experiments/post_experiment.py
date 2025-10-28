#!/usr/bin/env python3
#
# Copyright 2025 ProjectASAP contributors
# SPDX-License-Identifier: Apache-2.0
#


from utils.post.experiment_orchestrator import (
    ExperimentOrchestrator,
    ConfigurationManager,
)


def main():
    """Main entry point - minimal orchestration only."""
    # Setup
    ConfigurationManager.setup_logging()
    args = ConfigurationManager.parse_arguments()

    # Auto-detect query types from experiment folders
    query_types = ConfigurationManager.detect_query_types(args.experiments_dir)

    if not query_types:
        print(f"ERROR: No experiment folders found in {args.experiments_dir}")
        print("Expected folder names like: experiment_N_QUERYTYPE_ALGORITHM_...")
        return

    print(f"Auto-detected query types: {', '.join(query_types)}\n")

    from pathlib import Path

    # First, create global cross-query visualizations at the top level
    print(f"{'='*80}")
    print("Creating global cross-query visualizations")
    print(f"{'='*80}\n")

    global_orchestrator = ExperimentOrchestrator(
        experiments_dir=args.experiments_dir,
        output_dir=args.output_dir,
        sketch_type=query_types[0],  # Use first query type for parser
        compare_num=args.compare_num,
        memory_source=args.memory_source,
    )
    global_orchestrator.run_global_analysis()

    # Then process each query type separately in its own subdirectory
    for query_type in query_types:
        print(f"\n{'='*80}")
        print(f"Processing query type: {query_type}")
        print(f"{'='*80}\n")

        # Create query-specific output directory
        query_output_dir = Path(args.output_dir) / query_type

        orchestrator = ExperimentOrchestrator(
            experiments_dir=args.experiments_dir,
            output_dir=str(query_output_dir),
            sketch_type=query_type,
            compare_num=args.compare_num,
            memory_source=args.memory_source,
        )
        orchestrator.run_analysis(filter_by_query_type=True)


if __name__ == "__main__":
    main()

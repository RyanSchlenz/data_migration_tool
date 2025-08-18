#!/usr/bin/env python3
"""
Complete Migration Runner - No Subprocess Capture
Runs migration scripts directly without capturing output to avoid hanging
"""

import subprocess
import sys
import os
import glob
from migration_config import get_migration_config, get_project_info

def run_script_simple(script_name, phase_name):
    """Run script without capturing output - let it stream directly"""
    print(f"\n>>> PHASE {phase_name.upper()}: {script_name}")
    print("=" * 60)
    
    try:
        # Run without capture_output - let it stream directly to console
        result = subprocess.run([sys.executable, script_name])
        
        if result.returncode == 0:
            print(f"\n[OK] {phase_name.upper()} PHASE COMPLETED SUCCESSFULLY")
            return True
        else:
            print(f"\n[ERROR] {phase_name.upper()} PHASE FAILED")
            print(f"Return code: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"\n[ERROR] Error running {script_name}: {e}")
        return False

def check_files_exist(pattern, phase_name):
    """Check if required files exist for next phase"""
    files = glob.glob(pattern)
    if files:
        print(f"[OK] Found {len(files)} files for {phase_name} phase: {pattern}")
        # Show what files were actually found
        for file in files[:3]:  # Show first 3 files
            print(f"      ✓ {file}")
        if len(files) > 3:
            print(f"      ... and {len(files) - 3} more")
        return True
    else:
        print(f"[ERROR] No files found for {phase_name} phase: {pattern}")
        
        # Show what files ARE available to help debug
        all_json = glob.glob("*.json")
        if all_json:
            print(f"[DEBUG] Available JSON files:")
            for file in all_json:
                print(f"      - {file}")
        
        return False

def main():
    """Run complete migration workflow including subgraph recreation"""
    config = get_migration_config()
    project = get_project_info()
    
    print(f">>> {project['name'].upper()} COMPLETE MIGRATION RUNNER")
    print("=" * 60)
    print(f"Project: {project['name']} ({project['identifier']})")
    print(f"Dry Run: {config.get('dry_run', True)}")
    print(f"Source: Workspace {config.get('source', {}).get('workspace_id', 'Unknown')}")
    print(f"Target: Workspace {config.get('target', {}).get('workspace_id', 'Unknown')}")
    
    # Phase 1: Export
    print(f"\n>>> STARTING COMPLETE MIGRATION WORKFLOW")
    print(f"   Phase 1: Export subgraphs from source")
    print(f"   Phase 2: Create nodes in target")
    print(f"   Phase 3: Update metadata")
    print(f"   Phase 4: Recreate subgraphs with all nodes")
    
    success1 = run_script_simple('universal_subgraph_migration.py', 'EXPORT')
    
    if not success1:
        print(f"\n[STOP] Phase 1 failed - stopping workflow")
        return False
    
    # FIXED: Check for exported files with correct pattern
    # Use generic pattern that matches actual file naming: subgraph_*.json (but not subgraph_migration_*)
    export_pattern = "subgraph_*.json"
    
    # Filter out migration results manually
    all_subgraph_files = glob.glob(export_pattern)
    export_files = [f for f in all_subgraph_files if not f.startswith('subgraph_migration_')]
    
    if not export_files:
        print(f"\n[STOP] No exported EXPORT files found")
        print(f"   Looked for: {export_pattern} (excluding subgraph_migration_*)")
        print(f"   Found subgraph files: {len(all_subgraph_files)}")
        
        if all_subgraph_files:
            print(f"   Available subgraph files:")
            for file in all_subgraph_files:
                if file.startswith('subgraph_migration_'):
                    print(f"      - {file} (MIGRATION RESULT - not needed for next phase)")
                else:
                    print(f"      - {file} (EXPORT FILE - good for next phase)")
        
        print(f"\n[INFO] Need EXPORT files (subgraph_name_timestamp.json), not migration results")
        return False
    
    print(f"[OK] Found {len(export_files)} export files for CREATE phase")
    for file in export_files:
        print(f"      ✓ {file}")
    
    # Phase 2: Create
    success2 = run_script_simple('universal_node_creator.py', 'CREATE')
    
    if not success2:
        print(f"\n[STOP] Phase 2 failed - stopping workflow")
        return False
    
    # Check for creation files before Phase 3 - use generic pattern
    if not check_files_exist("*created_nodes*.json", "UPDATE"):
        print(f"\n[STOP] No creation result files found - stopping workflow")
        return False
    
    # Phase 3: Update
    success3 = run_script_simple('universal_metadata_updater.py', 'UPDATE')
    
    if not success3:
        print(f"\n[STOP] Phase 3 failed - stopping workflow")
        return False
    
    # Phase 4: Recreate Subgraphs (NEW!)
    print(f"\n>>> PHASE 4: Recreating subgraphs with all nodes (including manually created)")
    print(f"This will ensure your DV Stage and Source nodes are properly organized")
    
    success4 = run_script_simple('update_subgraph.py', 'RECREATE SUBGRAPHS')
    
    if not success4:
        print(f"\n[WARNING] Phase 4 failed - subgraphs may need manual organization")
        print(f"[INFO] The nodes were still created successfully in phases 1-3")
        print(f"[INFO] You can manually organize nodes into subgraphs in Coalesce UI")
    
    # Success summary
    print(f"\n>>> COMPLETE MIGRATION WORKFLOW FINISHED!")
    print("=" * 60)
    
    if success4:
        print(f"[OK] All 4 phases completed successfully")
        print(f"[OK] Subgraphs recreated with all nodes (API + manual)")
    else:
        print(f"[OK] First 3 phases completed successfully")
        print(f"[WARNING] Phase 4 (subgraph recreation) had issues")
    
    # Phase 5: Duplicate Cleanup
    success5 = run_script_simple('post_metadata_cleanup.py', 'DUPLICATE CLEANUP')
    if not success5:
        print(f"[WARNING] Phase 5 (duplicate cleanup) had issues - check for duplicates manually")
        
    
    # Check for manual action files - use generic patterns
    manual_patterns = [
        "*MANUAL_DOWNLOAD_REQUIRED*.txt",
        "*MANUAL_CREATE_REQUIRED*.txt", 
        "*MANUAL_UPDATE_REQUIRED*.txt",
        "*manual_downloads*.txt",
        "*manual_updates*.txt"
    ]
    
    manual_files = []
    for pattern in manual_patterns:
        manual_files.extend(glob.glob(pattern))
    
    if manual_files:
        print(f"\n[WARNING] Manual action files created:")
        for file in manual_files:
            print(f"  >> {file}")
        print(f"\n>> Review these files for manual actions needed in Coalesce UI")
    else:
        print(f"\n[OK] No manual actions required!")
    
    # Show created files - use generic patterns
    print(f"\n>> Files created:")
    result_patterns = [
        "subgraph_*.json",
        "*created_nodes*.json", 
        "*metadata_updates*.json"
    ]
    
    for pattern in result_patterns:
        files = glob.glob(pattern)
        if files:
            # For subgraph files, separate export vs migration results
            if pattern == "subgraph_*.json":
                export_files = [f for f in files if not f.startswith('subgraph_migration_')]
                migration_files = [f for f in files if f.startswith('subgraph_migration_')]
                
                if export_files:
                    latest_export = max(export_files, key=os.path.getmtime)
                    print(f"  >> {latest_export} (EXPORT)")
                if migration_files:
                    latest_migration = max(migration_files, key=os.path.getmtime) 
                    print(f"  >> {latest_migration} (MIGRATION RESULT)")
            else:
                latest = max(files, key=os.path.getmtime)
                print(f"  >> {latest}")
    
    workspace_id = config.get('target', {}).get('workspace_id', 'Unknown')
    print(f"\n>> FINAL STATUS:")
    
    if success4:
        print(f"✅ COMPLETE SUCCESS!")
        print(f"   1. Nodes created in workspace {workspace_id}")
        print(f"   2. Metadata updated")
        print(f"   3. Subgraphs recreated with all nodes")
        print(f"   4. Your DV Stage and Source nodes are properly organized")
        print(f"\n>> NEXT STEPS:")
        print(f"   1. Check Coalesce UI workspace {workspace_id}")
        print(f"   2. Test node executions")
        print(f"   3. Update config with new subgraph IDs (if provided)")
    else:
        print(f"✅ MOSTLY SUCCESS!")
        print(f"   1. Nodes created in workspace {workspace_id}")
        print(f"   2. Metadata updated")
        print(f"   ⚠️  Subgraph recreation had issues")
        print(f"\n>> NEXT STEPS:")
        print(f"   1. Check Coalesce UI workspace {workspace_id} for created nodes")
        print(f"   2. Review any manual action files above")
        print(f"   3. Manually organize nodes into subgraphs in Coalesce UI")
        print(f"   4. Test node executions")
    
    return success1 and success2 and success3  # Phase 4 is nice-to-have

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n[CANCELLED] Migration cancelled by user")
    except Exception as e:
        print(f"\n[ERROR] Migration failed: {e}")
        import traceback
        traceback.print_exc()
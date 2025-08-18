#!/usr/bin/env python3
"""
Enhanced Migration Configuration Template
Template for subgraph migration and verification projects
Copy this file to migration_config.py and customize for your specific project
"""

import os
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("[WARNING]  python-dotenv not installed. Install with: pip install python-dotenv")

# =============================================================================
# API CONFIGURATION - Loaded from .env file
# =============================================================================
def get_api_config():
    """Get API configuration from environment variables"""
    base_url = os.getenv('COALESCE_BASE_URL')
    access_token = os.getenv('COALESCE_ACCESS_TOKEN')
    
    if not base_url or not access_token:
        raise ValueError(
            "Missing required environment variables. Please check your .env file:\n"
            "  COALESCE_BASE_URL=https://your-instance.app.coalescesoftware.io\n"
            "  COALESCE_ACCESS_TOKEN=your_token_here"
        )
    
    return {
        "base_url": base_url,
        "access_token": access_token
    }

# =============================================================================
# MIGRATION CONFIGURATION TEMPLATE
# =============================================================================
MIGRATION_CONFIG = {
    # ============== PROJECT IDENTIFICATION ==============
    "project": {
        "name": "YOUR_PROJECT_NAME",                    # REPLACE: Your project name
        "identifier": "your_project_identifier",       # REPLACE: Used in filenames (no spaces)
        "description": "Your project description - migration from source to target workspace"  # REPLACE: Project description
    },
    
    # ============== SOURCE & TARGET WORKSPACES ==============
    "source": {
        "workspace_id": "SOURCE_WORKSPACE_ID",          # REPLACE: Your source workspace ID
        "workspace_name": "Source Workspace Name",     # REPLACE: Your source workspace name
        "project_name": "Source Project Name"          # REPLACE: Your source project name
    },
    
    "target": {
        "workspace_id": "TARGET_WORKSPACE_ID",          # REPLACE: Your target workspace ID
        "workspace_name": "Target Workspace Name",     # REPLACE: Your target workspace name
        "project_name": "Target Project Name"          # REPLACE: Your target project name
    },
    
    # ============== SUBGRAPHS WITH SOURCE/TARGET MAPPING ==============
    # Format: {"name": "X", "source_id": "Y", "target_id": "TBD"}
    "subgraphs": [
        {
            "name": "Subgraph Name 1",                  # REPLACE: Your subgraph name
            "source_id": "SOURCE_SUBGRAPH_ID_1",       # REPLACE: Source subgraph ID (numeric or UUID)
            "target_id": "TBD"                         # Leave as "TBD" - will be filled after migration
        },
        {
            "name": "Subgraph Name 2",                  # REPLACE: Your subgraph name
            "source_id": "SOURCE_SUBGRAPH_ID_2",       # REPLACE: Source subgraph ID (numeric or UUID)
            "target_id": "TBD"                         # Leave as "TBD" - will be filled after migration
        }
        # ADD MORE SUBGRAPHS AS NEEDED:
        # {
        #     "name": "Another Subgraph",
        #     "source_id": "ANOTHER_SOURCE_ID",
        #     "target_id": "TBD"
        # }
    ],
    
    # ============== JOBS/NODES OF INTEREST ==============
    "jobs_of_interest": [
        "Important Job Name 1",                        # REPLACE: Your important job/node names
        "Important Job Name 2",                        # REPLACE: Used for reference and documentation
        "Critical Process Name",                       # ADD: Any jobs you want to track specifically
        "Key Transformation",
        # ADD MORE JOB NAMES AS NEEDED
    ],
    
    # ============== MIGRATION BEHAVIOR ==============
    "migration_strategy": "subgraph_with_nodes",       # Keep as-is (standard strategy)
    "conflict_strategy": "rename",                     # Keep as-is (safe conflict resolution)
    "dry_run": True,                                   # CHANGE TO False when ready to execute
    "backup_before_migration": False,                  # Set to True if you want backups
    "validate_after_migration": True,                  # Keep as True (recommended)
    
    # ============== VERIFICATION BEHAVIOR ==============
    "verification": {
        "strict_name_matching": False,                 # Allow flexible name matching as fallback
        "check_node_details": True,                    # Compare node configurations
        "report_extra_nodes": True,                    # Report nodes in target not in source
        "ignore_test_nodes": True,                     # Skip nodes with test_, temp_, debug_ prefixes
        "use_target_ids": True                         # Use explicit target_id mapping when available
    },
    
    # ============== NODE FILTERING OPTIONS ==============
    "node_filters": {
        "exclude_patterns": ["temp_", "test_", "debug_"],  # Patterns to exclude from migration
        "include_only_types": [],                          # Leave empty to include all allowed types
        "max_nodes_per_subgraph": 100                      # Safety limit per subgraph
    },
    
    # ============== FILE NAMING PATTERNS ==============
    "file_patterns": {
        "exported_subgraph": "{identifier}_subgraph_{subgraph_name}_{timestamp}.json",
        "created_nodes": "{identifier}_nodes_created_{timestamp}.json", 
        "metadata_updates": "{identifier}_metadata_updates_{timestamp}.json",
        "migration_results": "{identifier}_migrated_{subgraph_name}_{new_id}.json",
        "verification_report": "{identifier}_verification_{timestamp}.json",
        "missing_nodes": "{identifier}_missing_nodes_{timestamp}.txt"
    }
}

# =============================================================================
# HELPER FUNCTIONS FOR MIGRATION & VERIFICATION
# =============================================================================

def get_migration_config():
    """Get the migration configuration"""
    return MIGRATION_CONFIG

def get_project_info():
    """Get project information from config"""
    config = get_migration_config()
    return config.get("project", {
        "name": "Unknown Project",
        "identifier": "unknown",
        "description": "Migration project"
    })

def get_file_pattern(pattern_type, **kwargs):
    """Get a file naming pattern with substitutions"""
    config = get_migration_config()
    project = get_project_info()
    
    patterns = config.get("file_patterns", {})
    pattern = patterns.get(pattern_type, f"{project['identifier']}_{pattern_type}_{{timestamp}}.json")
    
    # Add project info to kwargs
    kwargs.setdefault('identifier', project['identifier'])
    kwargs.setdefault('project_name', project['name'])
    
    return pattern.format(**kwargs)

def get_subgraphs_for_migration():
    """Get subgraphs configured for migration (source_id format for universal_subgraph_migration.py)"""
    config = get_migration_config()
    subgraphs = []
    
    for sg in config.get("subgraphs", []):
        if isinstance(sg, dict) and sg.get("source_id"):
            # Convert to universal migration format
            subgraphs.append({
                "name": sg["name"],
                "id": sg["source_id"]  # Use source_id as "id" for migration script
            })
    
    return subgraphs

def get_subgraphs_for_verification():
    """Get subgraphs configured for verification (both source_id and target_id)"""
    config = get_migration_config()
    verification_pairs = []
    
    for sg in config.get("subgraphs", []):
        if isinstance(sg, dict) and sg.get("source_id"):
            target_id = sg.get("target_id")
            
            # Only include if target_id is set and not "TBD"
            if target_id and target_id != "TBD":
                verification_pairs.append({
                    "name": sg["name"],
                    "source_id": sg["source_id"],
                    "target_id": target_id
                })
    
    return verification_pairs

def get_target_id_update_instructions(migration_results):
    """Generate instructions for updating target_id fields after migration"""
    instructions = []
    
    for result in migration_results:
        if result.get('migration_result') and result.get('original_subgraph'):
            original_name = result['original_subgraph'].get('subgraph_name', 'Unknown')
            new_id = result['migration_result'].get('data', {}).get('id')
            
            if new_id:
                instructions.append({
                    'subgraph_name': original_name,
                    'new_target_id': new_id,
                    'config_update': f'Set target_id: "{new_id}" for subgraph "{original_name}"'
                })
    
    return instructions

def validate_migration_config():
    """Validate the migration configuration"""
    errors = []
    
    # Check API config
    try:
        api_config = get_api_config()
    except ValueError as e:
        errors.append(str(e))
        return errors, 0, 0
    
    config = get_migration_config()
    
    # Check project info
    project = config.get("project", {})
    if not project.get("name") or project.get("name") == "YOUR_PROJECT_NAME":
        errors.append("Project name must be customized (replace YOUR_PROJECT_NAME)")
    if not project.get("identifier") or project.get("identifier") == "your_project_identifier":
        errors.append("Project identifier must be customized (replace your_project_identifier)")
    
    # Check source workspace
    source_id = config.get("source", {}).get("workspace_id")
    if not source_id or source_id == "SOURCE_WORKSPACE_ID":
        errors.append("Source workspace_id must be customized (replace SOURCE_WORKSPACE_ID)")
    
    # Check target workspace  
    target_id = config.get("target", {}).get("workspace_id")
    if not target_id or target_id == "TARGET_WORKSPACE_ID":
        errors.append("Target workspace_id must be customized (replace TARGET_WORKSPACE_ID)")
    
    # Check subgraphs
    subgraphs = config.get("subgraphs", [])
    if not subgraphs:
        errors.append("At least one subgraph must be specified")
    
    # Validate subgraph format
    migration_ready = 0
    verification_ready = 0
    
    for i, sg in enumerate(subgraphs):
        if not isinstance(sg, dict):
            errors.append(f"Subgraph {i+1} must be a dict with source_id and target_id")
            continue
            
        if not sg.get('name') or sg.get('name').startswith('Subgraph Name'):
            errors.append(f"Subgraph {i+1} name must be customized (replace 'Subgraph Name X')")
            
        source_id = sg.get('source_id')
        if not source_id or source_id.startswith('SOURCE_SUBGRAPH_ID'):
            errors.append(f"Subgraph {i+1} source_id must be customized (replace SOURCE_SUBGRAPH_ID_X)")
        else:
            migration_ready += 1
            
        target_id = sg.get('target_id')
        if target_id and target_id != "TBD":
            verification_ready += 1
    
    return errors, migration_ready, verification_ready

def print_migration_plan():
    """Print the migration and verification plan"""
    try:
        api_config = get_api_config()
        print(f">> API: {api_config['base_url']}")
        print(f"[KEY] Token: {api_config['access_token'][:10]}..." if api_config['access_token'] else "[KEY] Token: Not set")
    except Exception as e:
        print(f"[ERROR] API Config Error: {e}")
        return
    
    config = get_migration_config()
    project = get_project_info()
    
    print(f"\n>> {project['name'].upper()} CONFIGURATION")
    print("=" * 60)
    print(f">> Description: {project['description']}")
    print(f">>  Identifier: {project['identifier']}")
    
    # Source info
    source = config.get("source", {})
    print(f"\n>> SOURCE:")
    print(f"   Workspace: {source.get('workspace_name', 'Unknown')} (ID: {source.get('workspace_id', 'Unknown')})")
    print(f"   Project: {source.get('project_name', 'Unknown')}")
    
    # Target info
    target = config.get("target", {})
    print(f"\n>> TARGET:")
    print(f"   Workspace: {target.get('workspace_name', 'Unknown')} (ID: {target.get('workspace_id', 'Unknown')})")
    print(f"   Project: {target.get('project_name', 'Unknown')}")
    
    # Subgraph status
    print(f"\n>>> SUBGRAPH CONFIGURATION:")
    subgraphs = config.get("subgraphs", [])
    migration_ready = 0
    verification_ready = 0
    
    for sg in subgraphs:
        if isinstance(sg, dict):
            name = sg.get('name', 'Unknown')
            source_id = sg.get('source_id', 'No source_id')
            target_id = sg.get('target_id', 'TBD')
            
            if source_id != 'No source_id' and not source_id.startswith('SOURCE_SUBGRAPH_ID'):
                migration_ready += 1
                
            if target_id != 'TBD':
                verification_ready += 1
                print(f"   ✅ {name} (Source: {source_id} → Target: {target_id}) [MIGRATION & VERIFICATION READY]")
            else:
                print(f"   📄 {name} (Source: {source_id} → Target: {target_id}) [MIGRATION READY, VERIFICATION PENDING]")
    
    print(f"\n📊 STATUS:")
    print(f"   Migration ready: {migration_ready}/{len(subgraphs)} subgraphs")
    print(f"   Verification ready: {verification_ready}/{len(subgraphs)} subgraphs")
    
    if verification_ready < migration_ready:
        print(f"\n💡 TO ENABLE VERIFICATION:")
        print(f"   1. Run migration: python run_migration.py")
        print(f"   2. Note the new target subgraph IDs from migration output")
        print(f"   3. Update target_id fields from 'TBD' to actual IDs")
        print(f"   4. Run verification: python migration_verification.py")
    
    # Migration settings
    print(f"\n>> MIGRATION SETTINGS:")
    print(f"   Strategy: {config.get('migration_strategy', 'default')}")
    print(f"   Conflicts: {config.get('conflict_strategy', 'rename')}")
    print(f"   Dry Run: {config.get('dry_run', True)}")
    
    # Jobs of interest
    jobs = config.get("jobs_of_interest", [])
    if jobs:
        print(f"\n>>> JOBS OF INTEREST:")
        for job in jobs:
            print(f"   * {job}")

def check_env_file():
    """Check if .env file exists and has required variables"""
    env_file = Path('.env')
    
    if not env_file.exists():
        print("[ERROR] .env file not found")
        print("\n>> Create a .env file with:")
        print("COALESCE_BASE_URL=https://your-instance.app.coalescesoftware.io")
        print("COALESCE_ACCESS_TOKEN=your_token_here")
        return False
    
    # Check if required variables are set
    missing_vars = []
    
    if not os.getenv('COALESCE_BASE_URL'):
        missing_vars.append('COALESCE_BASE_URL')
    
    if not os.getenv('COALESCE_ACCESS_TOKEN'):
        missing_vars.append('COALESCE_ACCESS_TOKEN')
    
    if missing_vars:
        print(f"[ERROR] Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("[OK] .env file found with required variables")
    return True

if __name__ == "__main__":
    """Test configuration when run directly"""
    project = get_project_info()
    
    print(f">>> {project['name'].upper()} CONFIGURATION TEMPLATE TEST")
    print("=" * 60)
    
    if check_env_file():
        errors, migration_ready, verification_ready = validate_migration_config()
        
        if errors:
            print("\n[ERROR] Configuration template needs customization:")
            for error in errors:
                print(f"   - {error}")
            print(f"\n🎯 REQUIRED CUSTOMIZATIONS:")
            print(f"   1. Replace YOUR_PROJECT_NAME with your actual project name")
            print(f"   2. Replace SOURCE_WORKSPACE_ID with your source workspace ID")
            print(f"   3. Replace TARGET_WORKSPACE_ID with your target workspace ID")
            print(f"   4. Replace SOURCE_SUBGRAPH_ID_X with your actual subgraph IDs")
            print(f"   5. Update subgraph names and job names for your project")
            print(f"   6. Set dry_run: False when ready to execute")
        else:
            print("\n[OK] Configuration template validated!")
        
        print_migration_plan()
        
        print(f"\n>> TEMPLATE SETUP WORKFLOW:")
        print(f"   1. Copy: cp template_migration_config.py migration_config.py")
        print(f"   2. Customize: Edit migration_config.py with your values")
        print(f"   3. Discover: python coalesce_discovery.py (find workspace/subgraph IDs)")
        print(f"   4. Test: python migration_config.py (validate configuration)")
        print(f"   5. Execute: python run_migration.py (start migration)")
        
        print(f"\n>> DISCOVERY TOOLS:")
        print(f"   • python coalesce_discovery.py - Find workspaces and subgraphs")
        print(f"   • python check_node_types.py - Analyze node types in data")
        print(f"   • python node_structure_inspector.py - Debug node structures")
        
        print(f"\n>> TEMPLATE FEATURES:")
        print(f"   ✅ No hardcoded values - everything is configurable")
        print(f"   ✅ Project-agnostic - works for any Coalesce instance")
        print(f"   ✅ Enhanced validation - catches missing customizations")
        print(f"   ✅ Comprehensive helper functions for all scripts")
        print(f"   ✅ Support for both numeric and UUID subgraph IDs")
        print(f"   ✅ Flexible file naming patterns")
        print(f"   ✅ Built-in verification configuration")
        
        print(f"\n🎯 READY TO CUSTOMIZE:")
        print(f"   This is a TEMPLATE - replace all placeholder values!")
        print(f"   See comments marked with REPLACE: for required changes")
        print(f"   Test your configuration before running actual migration")
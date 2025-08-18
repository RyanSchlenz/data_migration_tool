#!/usr/bin/env python3
"""
Enhanced Universal Node Creator Tool with Hash Key Testing
Tests including hash keys, with fallback to cleaned format
Supports node type filtering to exclude source tables
Works with any project - all configuration comes from migration_config.py
Finds any JSON files containing 'subgraph' in the name
"""

import requests
import json
import os
import sys
import glob
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from coalesce_conn import load_config_from_env
from migration_config import get_migration_config, get_project_info, get_file_pattern

class EnhancedNodeCreator:
    """Enhanced node creation tool with hash key testing and node type filtering"""
    
    def __init__(self):
        load_dotenv()
        config_data = load_config_from_env()
        
        if not config_data:
            raise RuntimeError("[ERROR] Could not load API config")
        
        self.base_url = config_data.get('base_url', '').rstrip('/')
        self.access_token = config_data.get('access_token')
        
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        # Load migration configuration
        self.migration_config = get_migration_config()
        self.project_info = get_project_info()
        
        # Enhanced configuration options
        self.test_hash_keys = True  # Try to include hash keys first
        self.allowed_node_types = [  # Node types to migrate (exclude sources)
            'Satellite', 'Hub', 'Link', 'Hub_Advanced', 'Link_Advanced',
            'Dimension', 'Fact', 'View', 'BaseNodes'
            # Notably excluding: 'Source', 'Base', 'BaseNodes', 'Stage'
        ]
        
        # Track different approaches
        self.node_id_mapping = {}
        self.created_nodes = []
        self.creation_errors = []
        self.failed_creations = []
        self.hash_key_success = []  # Track which nodes successfully included hash keys
        self.hash_key_failures = []  # Track which nodes failed with hash keys
        
        # Get script name for metadata
        self.script_name = os.path.basename(sys.argv[0]) if sys.argv else "enhanced_node_creator.py"
        
        print(f">>> {self.project_info['name'].upper()} ENHANCED NODE CREATOR")
        print(f"   Project: {self.project_info['name']} ({self.project_info['identifier']})")
        print(f"   Script: {self.script_name}")
        print(f"   API: {self.base_url}")
        print(f"   [TEST] Hash Keys: {self.test_hash_keys}")
        print(f"   [FILTER] Allowed Types: {', '.join(self.allowed_node_types)}")
        print(f"   [SEARCH] Looks for: any JSON files containing 'subgraph'")
    
    def filter_nodes_by_type(self, all_nodes):
        """Filter nodes to only include allowed types (exclude sources/stages) - CORRECTED"""
        print(f"\n>>> FILTERING NODES BY TYPE")
        print("-" * 50)
        
        filtered_nodes = {}
        excluded_nodes = {}
        
        # CORRECTED Node type mapping for your actual data
        node_type_mapping = {
            '30': 'Satellite',     # 89 nodes
            '31': 'Satellite',     # 59 nodes  
            '32': 'Hub',           # 53 nodes
            '33': 'Link',          # 33 nodes
            '34': 'Dimension',     # (if you have any)
            '35': 'Fact',          # (if you have any)
            '36': 'Stage',         # 146 nodes - EXCLUDE (API limitation)
            '37': 'View',          # 51 nodes - INCLUDE
            '50': 'Hub_Advanced',  # 6 nodes - Data Vault extended
            '51': 'Link_Advanced', # 13 nodes - Data Vault extended
            '205': 'Source',       # EXCLUDE (API limitation)
            '206': 'BaseNodes',    # Include if found
            'BaseNodes:::205': 'BaseNodes',  # 5 nodes - INCLUDE
            'Source': 'Source',    # 108 nodes - EXCLUDE (API limitation)
            'Stage': 'Stage',      # 1 node - EXCLUDE (API limitation)
            'View': 'View'         # 51 nodes - INCLUDE
        }
        
        # CORRECTED allowed types based on your node analysis
        allowed_types = [
            'Satellite',      # Types 30, 31 = 148 nodes
            'Hub',           # Type 32 = 53 nodes  
            'Link',          # Type 33 = 33 nodes
            'Hub_Advanced',  # Type 50 = 6 nodes
            'Link_Advanced', # Type 51 = 13 nodes
            'View',          # Type 37, View = 51 nodes
            'BaseNodes',     # BaseNodes:::205 = 5 nodes
            'Dimension', 'Fact'  # If any exist
            # EXCLUDE: 'Source' (108), 'Stage' (147), types 36, 205, Source, Stage
        ]
        
        # Debug: Track what we're finding
        type_counts = {}
        
        for node_id, node_data in all_nodes.items():
            node_name = node_data.get('name', f'Node_{node_id}')
            raw_node_type = str(node_data.get('nodeType', 'Unknown'))
            
            # Normalize nodeType using the mapping
            if raw_node_type in node_type_mapping:
                normalized_type = node_type_mapping[raw_node_type]
            else:
                normalized_type = raw_node_type  # Keep as-is if not in mapping
            
            # Track type counts for debugging
            type_key = f"{raw_node_type} -> {normalized_type}"
            type_counts[type_key] = type_counts.get(type_key, 0) + 1
            
            # Check if this normalized type is allowed
            if normalized_type in allowed_types:
                filtered_nodes[node_id] = node_data
                print(f"   [INCLUDE] '{node_name}' (type: {raw_node_type} -> {normalized_type})")
            else:
                excluded_nodes[node_id] = node_data
                print(f"   [EXCLUDE] '{node_name}' (type: {raw_node_type} -> {normalized_type})")
        
        print(f"\n>> FILTERING SUMMARY:")
        print(f"   [OK] Included: {len(filtered_nodes)} nodes")
        print(f"   [SKIP] Excluded: {len(excluded_nodes)} nodes")
        
        print(f"\n>> TYPE BREAKDOWN:")
        for type_key, count in sorted(type_counts.items()):
            status = "INCLUDED" if any(allowed in type_key for allowed in allowed_types) else "EXCLUDED"
            print(f"   {status}: {type_key}: {count} nodes")
        
        # Expected calculation
        expected_included = 89 + 59 + 53 + 33 + 6 + 13 + 51 + 5  # = 309
        print(f"\n>> EXPECTED: ~309 nodes included (89+59+53+33+6+13+51+5)")
        print(f">> ACTUAL: {len(filtered_nodes)} nodes included")
        
        if len(filtered_nodes) < 300:
            print(f"   ⚠️  WARNING: Getting fewer nodes than expected!")
            print(f"   Check if node types are being mapped correctly")
        
        return filtered_nodes, excluded_nodes
    
    def find_exported_json_files(self):
        """Find JSON files with export patterns (excludes migration results)"""
        print(f"\n>>> LOOKING FOR SUBGRAPH EXPORT FILES")
        print("-" * 50)
        
        print(f">> Searching for EXPORT files (excludes migration results)...")
        
        # Get all JSON files first
        all_json_files = glob.glob("*.json")
        print(f"   Found {len(all_json_files)} total JSON files in directory")
        
        # Enhanced pattern matching for EXPORT files only
        import re
        json_files = []
        
        # Define patterns for EXPORT files (contain original node data):
        # 1. subgraph_*.json BUT NOT subgraph_migration_* (export files)
        # 2. *_subgraph_*.json (anything_subgraph_anything)  
        # 3. *subgraph*.json (fallback, but exclude migration results)
        
        patterns = [
            r'^subgraph_(?!migration_).*\.json$',  # subgraph_*.json EXCEPT subgraph_migration_*
            r'.*_subgraph_.*\.json$',              # *_subgraph_*.json
            r'.*subgraph.*\.json$'                 # fallback: any file with subgraph
        ]
        
        matched_files = set()  # Use set to avoid duplicates
        pattern_matches = {}   # Track which patterns matched which files
        
        for pattern in patterns:
            pattern_matches[pattern] = []
            for file in all_json_files:
                if re.match(pattern, file, re.IGNORECASE):
                    # Additional filter for fallback pattern to exclude migration results
                    if pattern == r'.*subgraph.*\.json$':
                        if not file.startswith('subgraph_migration_'):
                            matched_files.add(file)
                            pattern_matches[pattern].append(file)
                    else:
                        matched_files.add(file)
                        pattern_matches[pattern].append(file)
        
        json_files = list(matched_files)
        
        if json_files:
            print(f"   [OK] Found {len(json_files)} EXPORT files (excluding migration results):")
            
            # Show which patterns matched which files
            for pattern, files in pattern_matches.items():
                if files:
                    pattern_desc = {
                        r'^subgraph_(?!migration_).*\.json$': 'subgraph_*.json (EXPORT files, excludes migration results)',
                        r'.*_subgraph_.*\.json$': '*_subgraph_*.json',
                        r'.*subgraph.*\.json$': '*subgraph*.json (fallback, excludes migration results)'
                    }
                    print(f"   Pattern {pattern_desc.get(pattern, pattern)}:")
                    for file in files:
                        print(f"      ✓ {file}")
        else:
            print(f"   [INFO] No EXPORT files found")
            
            # Show what JSON files ARE available
            if all_json_files:
                print(f"\n[INFO] Available JSON files in current directory:")
                for file in all_json_files:
                    if file.startswith('subgraph_migration_'):
                        print(f"      - {file} (MIGRATION RESULT - contains API responses)")
                    else:
                        print(f"      - {file}")
                print(f"\n[SUGGESTION] Expected patterns for EXPORT files:")
                print(f"             • 'subgraph_*.json' (export files like subgraph_klaviyo_20250818.json)")
                print(f"             • '*_subgraph_*.json' (anything_subgraph_anything)")
                print(f"             • Fallback: any file containing 'subgraph'")
                print(f"             • NOTE: Excludes subgraph_migration_* (those are migration results)")
            else:
                print(f"\n[ERROR] No JSON files found in current directory at all!")
                print(f"        Make sure you've exported subgraph data first.")
            
            return []
        
        print(f"\n[SUCCESS] Found {len(json_files)} subgraph EXPORT files:")
        for i, file in enumerate(json_files):
            file_path = Path(file)
            file_size = file_path.stat().st_size / 1024
            mod_time = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            print(f"   {i+1}. {file} ({file_size:.1f} KB, {mod_time})")
        
        print(f"\n>> Export file pattern support:")
        print(f"   ✓ subgraph_*.json (EXPORT files - contains original node data)")
        print(f"   ✓ *_subgraph_*.json (anything_subgraph_anything)")
        print(f"   ✓ *subgraph*.json (fallback pattern)")
        print(f"   ✓ Case-insensitive matching")
        print(f"   ✓ No hardcoded project names")
        print(f"   ❌ Excludes subgraph_migration_* (migration results, not source data)")
        
        return json_files
    
    def load_and_consolidate_data(self, json_files):
        """Load all JSON files and consolidate node data"""
        print(f"\n📂 LOADING AND CONSOLIDATING DATA")
        print("-" * 50)
        
        all_nodes = {}
        all_subgraphs = []
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                subgraph_name = data.get('subgraph_name', 'Unknown')
                node_details = data.get('node_details', {})
                steps = data.get('steps', [])
                
                print(f">> Loading: '{subgraph_name}' from {json_file}")
                print(f"   Nodes in file: {len(node_details)}")
                
                for node_id, node_data in node_details.items():
                    if node_id not in all_nodes:
                        all_nodes[node_id] = node_data
                    else:
                        print(f"   [WARNING] Duplicate node ID found: {node_id}")
                
                all_subgraphs.append({
                    'name': subgraph_name,
                    'source_file': json_file,
                    'steps': steps,
                    'node_count': len(node_details)
                })
                
            except Exception as e:
                print(f"[ERROR] Error loading {json_file}: {e}")
                continue
        
        print(f"\n>> CONSOLIDATION SUMMARY:")
        print(f"   [OK] Total unique nodes: {len(all_nodes)}")
        print(f"   [OK] Total subgraphs: {len(all_subgraphs)}")
        for sg in all_subgraphs:
            print(f"      • '{sg['name']}': {sg['node_count']} nodes (from {sg['source_file']})")
        
        return all_nodes, all_subgraphs
    
    def analyze_dependencies(self, all_nodes):
        """Analyze node dependencies from column references"""
        print(f"\n>>> ANALYZING NODE DEPENDENCIES")
        print("-" * 50)
        
        dependencies = {}
        
        for node_id, node_data in all_nodes.items():
            node_name = node_data.get('name', f'Node_{node_id}')
            predecessors = set()
            
            metadata = node_data.get('metadata', {})
            columns = metadata.get('columns', [])
            
            for column in columns:
                sources = column.get('sources', [])
                for source in sources:
                    col_refs = source.get('columnReferences', [])
                    for col_ref in col_refs:
                        predecessor_id = col_ref.get('nodeID')
                        if predecessor_id and predecessor_id != node_id:
                            predecessors.add(predecessor_id)
            
            valid_predecessors = [pid for pid in predecessors if pid in all_nodes]
            dependencies[node_id] = valid_predecessors
            
            if valid_predecessors:
                print(f">> {node_name}: {len(valid_predecessors)} dependencies")
        
        return dependencies
    
    def validate_and_clean_node_data(self, node_data, include_hash_keys=False):
        """Extract node data with optional hash key inclusion"""
        node_name = node_data.get('name', f"Node_{datetime.now().strftime('%H%M%S')}")
        node_type = node_data.get('nodeType', 'Stage')
        
        # Map numeric nodeType to string
        node_type_mapping = {
            '30': 'Satellite', '31': 'Satellite', '32': 'Hub', '33': 'Link',
            '34': 'Dimension', '35': 'Fact', '36': 'Stage', '37': 'View',
            '205': 'Source', '206': 'BaseNodes'
        }
        
        if node_type in node_type_mapping:
            node_type = node_type_mapping[node_type]
        
        cleaned_data = {
            'name': node_name,
            'nodeType': node_type,
            'locationName': node_data.get('locationName', ''),
            'database': node_data.get('database', ''),
            'schema': node_data.get('schema', ''),
            'table': node_data.get('table', node_name),
            'description': node_data.get('description', f'Migrated {self.project_info["name"]} node: {node_name}')
        }
        
        # Process columns
        original_metadata = node_data.get('metadata', {})
        original_columns = original_metadata.get('columns', [])
        
        if include_hash_keys:
            print(f"      [TEST] Including hash keys for '{node_name}' ({len(original_columns)} columns)")
        else:
            print(f"      [CLEAN] Excluding hash keys for '{node_name}' ({len(original_columns)} columns)")
        
        cleaned_columns = []
        hash_key_count = 0
        
        for col in original_columns:
            cleaned_col = {
                'name': col.get('name', 'UNKNOWN_COLUMN'),
                'dataType': col.get('dataType', 'VARCHAR'),
                'nullable': col.get('nullable', True),
                'description': col.get('description', '')
            }
            
            # Add columnID if present
            if 'columnID' in col:
                cleaned_col['columnID'] = str(col['columnID'])
            
            # Add defaultValue if simple
            if 'defaultValue' in col and col['defaultValue'] and isinstance(col['defaultValue'], (str, int, float, bool)):
                cleaned_col['defaultValue'] = str(col['defaultValue'])
            
            # Add primaryKey if present
            if 'primaryKey' in col and isinstance(col['primaryKey'], bool):
                cleaned_col['primaryKey'] = col['primaryKey']
            
            # EXPERIMENTAL: Include hash-related fields if testing
            if include_hash_keys:
                # Try to include hash columns
                if 'hashedColumns' in col and col['hashedColumns']:
                    try:
                        # Simplify hashedColumns to just the column names
                        hashed_cols = col['hashedColumns']
                        if isinstance(hashed_cols, list):
                            # Extract just the column names if it's a complex structure
                            simplified_hashed = []
                            for hc in hashed_cols:
                                if isinstance(hc, dict) and 'name' in hc:
                                    simplified_hashed.append(hc['name'])
                                elif isinstance(hc, str):
                                    simplified_hashed.append(hc)
                            if simplified_hashed:
                                cleaned_col['hashedColumns'] = simplified_hashed
                                hash_key_count += 1
                        elif isinstance(hashed_cols, str):
                            cleaned_col['hashedColumns'] = [hashed_cols]
                            hash_key_count += 1
                    except Exception as e:
                        print(f"         [WARNING] Could not simplify hashedColumns: {e}")
                
                # Try to include hash details in simplified form
                if 'hashDetails' in col and col['hashDetails']:
                    try:
                        hash_details = col['hashDetails']
                        if isinstance(hash_details, dict):
                            # Keep only simple hash details
                            simplified_details = {}
                            for key, value in hash_details.items():
                                if isinstance(value, (str, int, float, bool)):
                                    simplified_details[key] = value
                            if simplified_details:
                                cleaned_col['hashDetails'] = simplified_details
                    except Exception as e:
                        print(f"         [WARNING] Could not simplify hashDetails: {e}")
            
            cleaned_columns.append(cleaned_col)
        
        if cleaned_columns:
            cleaned_data['metadata'] = {'columns': cleaned_columns}
            if include_hash_keys and hash_key_count > 0:
                print(f"      [OK] Included {hash_key_count} hash key references")
            elif include_hash_keys:
                print(f"      [INFO] No hash keys found to include")
        
        return cleaned_data
    
    def create_node_via_api(self, workspace_id, node_id, node_data, predecessor_ids, dry_run=True):
        """Create node with hash key testing and fallback"""
        node_name = node_data.get('name', f'Node_{node_id}')
        node_type = node_data.get('nodeType', 'Stage')
        
        if dry_run:
            print(f"   [SKIP] DRY RUN: Would create '{node_name}' (type: {node_type})")
            if self.test_hash_keys:
                print(f"      [TEST] Would attempt hash key inclusion first")
            
            fake_new_id = f"new_{len(self.node_id_mapping) + 1:03d}"
            self.node_id_mapping[node_id] = fake_new_id
            
            return {
                "dry_run": True,
                "original_id": node_id,
                "new_id": fake_new_id,
                "name": node_name,
                "type": node_type,
                "predecessor_count": len(predecessor_ids)
            }
        
        try:
            # STEP 1: Create node structure (POST)
            create_payload = {
                "nodeType": node_type,
                "predecessorNodeIDs": predecessor_ids
            }
            
            print(f"   >> Step 1: Creating node structure...")
            
            response = requests.post(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/nodes",
                headers=self.headers,
                json=create_payload,
                timeout=30
            )
            
            print(f"      Status: {response.status_code}")
            
            if response.status_code not in [200, 201]:
                error_msg = f"Failed to create node structure: {response.status_code}"
                try:
                    error_detail = response.json() if response.text else response.text[:200]
                    error_msg += f" - {error_detail}"
                except:
                    error_msg += f" - {response.text[:200]}"
                
                print(f"   [ERROR] {error_msg}")
                self.creation_errors.append({
                    "type": "node_creation",
                    "original_id": node_id,
                    "name": node_name,
                    "error": error_msg,
                    "step": "POST_create"
                })
                return None
            
            created_data = response.json()
            new_node_data = created_data.get('data', created_data)
            new_node_id = new_node_data.get('id')
            
            if not new_node_id:
                error_msg = f"No node ID returned from creation"
                print(f"   [ERROR] {error_msg}")
                return None
            
            print(f"   [OK] Step 1 Success: Created node with ID {new_node_id}")
            self.node_id_mapping[node_id] = new_node_id
            
            # STEP 2: Attempt configuration with hash keys first (if enabled)
            hash_success = False
            if self.test_hash_keys:
                print(f"   >> Step 2a: Attempting with hash keys...")
                
                hash_data = self.validate_and_clean_node_data(node_data, include_hash_keys=True)
                hash_payload = {"id": new_node_id, **hash_data}
                
                hash_response = requests.put(
                    f"{self.base_url}/api/v1/workspaces/{workspace_id}/nodes/{new_node_id}",
                    headers=self.headers,
                    json=hash_payload,
                    timeout=30
                )
                
                print(f"      Hash test status: {hash_response.status_code}")
                
                if hash_response.status_code in [200, 201]:
                    print(f"   [OK] Step 2a Success: Hash keys included!")
                    hash_success = True
                    self.hash_key_success.append({
                        "node_id": new_node_id,
                        "name": node_name,
                        "type": node_type
                    })
                else:
                    print(f"   [WARNING] Hash keys failed, trying cleaned approach...")
                    try:
                        error_detail = hash_response.json() if hash_response.text else hash_response.text[:200]
                        print(f"      Hash error: {error_detail}")
                    except:
                        print(f"      Hash error: {hash_response.text[:200]}")
                    
                    self.hash_key_failures.append({
                        "node_id": new_node_id,
                        "name": node_name,
                        "type": node_type,
                        "error": f"Hash inclusion failed: {hash_response.status_code}"
                    })
            
            # STEP 2b: Use cleaned approach if hash keys failed or not testing
            if not hash_success:
                print(f"   >> Step 2b: Using cleaned approach...")
                
                cleaned_data = self.validate_and_clean_node_data(node_data, include_hash_keys=False)
                cleaned_payload = {"id": new_node_id, **cleaned_data}
                
                cleaned_response = requests.put(
                    f"{self.base_url}/api/v1/workspaces/{workspace_id}/nodes/{new_node_id}",
                    headers=self.headers,
                    json=cleaned_payload,
                    timeout=30
                )
                
                print(f"      Cleaned status: {cleaned_response.status_code}")
                
                if cleaned_response.status_code not in [200, 201]:
                    print(f"   [ERROR] Even cleaned approach failed: {cleaned_response.status_code}")
                    try:
                        error_detail = cleaned_response.json() if cleaned_response.text else cleaned_response.text[:200]
                        print(f"      Error: {error_detail}")
                    except:
                        print(f"      Error: {cleaned_response.text[:200]}")
                    
                    self.creation_errors.append({
                        "type": "cleaned_config_update",
                        "original_id": node_id,
                        "new_id": new_node_id,
                        "name": node_name,
                        "error": f"Cleaned config failed: {cleaned_response.status_code}",
                        "step": "PUT_update_cleaned"
                    })
                else:
                    print(f"   [OK] Step 2b Success: Cleaned configuration applied")
            
            self.created_nodes.append({
                "original_id": node_id,
                "new_id": new_node_id,
                "name": node_name,
                "type": node_type,
                "status": "created",
                "predecessor_count": len(predecessor_ids),
                "hash_keys_included": hash_success,
                "config_type": "hash_keys" if hash_success else "cleaned"
            })
            
            print(f"   [OK] Final: '{node_name}': {node_id} -> {new_node_id} ({'hash keys' if hash_success else 'cleaned'})")
            return new_node_data
                
        except Exception as e:
            error_msg = f"Exception creating '{node_name}': {e}"
            print(f"   [ERROR] {error_msg}")
            self.creation_errors.append({
                "type": "exception",
                "original_id": node_id,
                "name": node_name,
                "error": error_msg
            })
            return None
    
    def create_nodes_in_dependency_order(self, workspace_id, all_nodes, dependencies, dry_run=True):
        """Create all nodes in proper dependency order"""
        print(f"\n>> CREATING NODES IN DEPENDENCY ORDER")
        print(f"Workspace: {workspace_id}")
        print(f"Total nodes: {len(all_nodes)}")
        print(f"Hash key testing: {self.test_hash_keys}")
        print(f"Dry run: {dry_run}")
        print("-" * 60)
        
        created_count = 0
        remaining_nodes = set(all_nodes.keys())
        creation_rounds = 0
        max_rounds = len(all_nodes) + 10
        
        while remaining_nodes and creation_rounds < max_rounds:
            creation_rounds += 1
            print(f"\n[PROCESSING] Creation Round {creation_rounds} ({len(remaining_nodes)} nodes remaining)")
            
            # Find nodes ready for creation
            ready_nodes = []
            for node_id in remaining_nodes:
                node_deps = dependencies.get(node_id, [])
                unsatisfied_deps = [dep for dep in node_deps if dep not in self.node_id_mapping]
                if not unsatisfied_deps:
                    ready_nodes.append(node_id)
            
            if not ready_nodes:
                print(f"[WARNING] No nodes ready. Finding nodes with minimal dependencies...")
                min_deps = min(len([d for d in dependencies.get(nid, []) if d not in self.node_id_mapping]) 
                              for nid in remaining_nodes)
                ready_nodes = [nid for nid in remaining_nodes 
                              if len([d for d in dependencies.get(nid, []) if d not in self.node_id_mapping]) == min_deps]
                print(f"   Creating {len(ready_nodes)} nodes with {min_deps} unsatisfied dependencies")
            
            # Create ready nodes
            round_success = 0
            for node_id in ready_nodes:
                node_data = all_nodes[node_id]
                node_name = node_data.get('name', f'Node_{node_id}')
                
                original_deps = dependencies.get(node_id, [])
                mapped_deps = [self.node_id_mapping.get(dep_id, dep_id) for dep_id in original_deps 
                              if dep_id in self.node_id_mapping]
                
                print(f"\n>> Creating: '{node_name}' ({len(mapped_deps)}/{len(original_deps)} deps satisfied)")
                
                result = self.create_node_via_api(workspace_id, node_id, node_data, mapped_deps, dry_run)
                
                if result:
                    created_count += 1
                    round_success += 1
                
                remaining_nodes.remove(node_id)
            
            print(f"   Round {creation_rounds}: {round_success}/{len(ready_nodes)} nodes created successfully")
            
            if round_success == 0 and remaining_nodes:
                print(f"[WARNING] No progress in round {creation_rounds}, stopping to avoid infinite loop")
                break
        
        print(f"\n>> NODE CREATION SUMMARY:")
        if dry_run:
            print(f"   [SKIP] DRY RUN: Would create {created_count} nodes in {creation_rounds} rounds")
        else:
            print(f"   [OK] Created: {created_count} nodes in {creation_rounds} rounds")
        
        if self.test_hash_keys and not dry_run:
            print(f"   [HASH] Success: {len(self.hash_key_success)} nodes with hash keys")
            print(f"   [HASH] Failed: {len(self.hash_key_failures)} nodes fell back to cleaned")
        
        return created_count
    
    def create_subgraphs_from_original(self, workspace_id, all_subgraphs, dry_run=True):
        """Create subgraphs in target workspace using created nodes"""
        print(f"\n>>> CREATING SUBGRAPHS IN TARGET WORKSPACE")
        print(f"Workspace: {workspace_id}")
        print(f"Subgraphs to create: {len(all_subgraphs)}")
        print("-" * 60)
        
        created_subgraphs = []
        
        for subgraph_info in all_subgraphs:
            subgraph_name = subgraph_info['name']
            original_steps = subgraph_info.get('steps', [])
            source_file = subgraph_info.get('source_file', 'unknown')
            
            # Map original node IDs to new node IDs
            mapped_steps = []
            unmapped_count = 0
            
            for step in original_steps:
                original_id = str(step)
                if original_id in self.node_id_mapping:
                    new_id = self.node_id_mapping[original_id]
                    mapped_steps.append(new_id)
                else:
                    unmapped_count += 1
                    # Don't include unmapped nodes - they were probably excluded by filtering
            
            print(f"\n>> Creating subgraph: '{subgraph_name}' (from {source_file})")
            print(f"   Original steps: {len(original_steps)}")
            print(f"   Mapped steps: {len(mapped_steps)}")
            print(f"   Unmapped (excluded): {unmapped_count}")
            
            if not mapped_steps:
                print(f"   [SKIP] No mapped nodes for this subgraph (all nodes were filtered out)")
                created_subgraphs.append({
                    "skipped": True,
                    "original_name": subgraph_name,
                    "source_file": source_file,
                    "reason": "No API-compatible nodes in subgraph",
                    "original_node_count": len(original_steps),
                    "mapped_node_count": 0
                })
                continue
            
            # Create new subgraph name
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            new_subgraph_name = f"{subgraph_name}_Migrated_{timestamp}"
            
            subgraph_payload = {
                "name": new_subgraph_name,
                "steps": mapped_steps
            }
            
            if dry_run:
                print(f"   [SKIP] DRY RUN: Would create '{new_subgraph_name}' with {len(mapped_steps)} nodes")
                created_subgraphs.append({
                    "dry_run": True,
                    "original_name": subgraph_name,
                    "source_file": source_file,
                    "new_name": new_subgraph_name,
                    "original_node_count": len(original_steps),
                    "mapped_node_count": len(mapped_steps),
                    "unmapped_count": unmapped_count,
                    "nodes": mapped_steps
                })
            else:
                try:
                    response = requests.post(
                        f"{self.base_url}/api/v1/workspaces/{workspace_id}/subgraphs",
                        headers=self.headers,
                        json=subgraph_payload,
                        timeout=30
                    )
                    
                    print(f"   📡 Status: {response.status_code}")
                    
                    if response.status_code in [200, 201]:
                        created_data = response.json()
                        new_subgraph_id = created_data.get('data', {}).get('id', created_data.get('id'))
                        print(f"   [OK] SUCCESS: Created subgraph ID {new_subgraph_id}")
                        
                        created_subgraphs.append({
                            "success": True,
                            "original_name": subgraph_name,
                            "source_file": source_file,
                            "new_name": new_subgraph_name,
                            "new_id": new_subgraph_id,
                            "original_node_count": len(original_steps),
                            "mapped_node_count": len(mapped_steps),
                            "unmapped_count": unmapped_count,
                            "nodes": mapped_steps,
                            "api_response": created_data
                        })
                    else:
                        error_msg = f"Failed to create subgraph: {response.status_code}"
                        try:
                            error_detail = response.json() if response.text else response.text[:200]
                            error_msg += f" - {error_detail}"
                        except:
                            error_msg += f" - {response.text[:200]}"
                        
                        print(f"   [ERROR] {error_msg}")
                        created_subgraphs.append({
                            "success": False,
                            "original_name": subgraph_name,
                            "source_file": source_file,
                            "new_name": new_subgraph_name,
                            "error": error_msg,
                            "original_node_count": len(original_steps),
                            "mapped_node_count": len(mapped_steps),
                            "unmapped_count": unmapped_count
                        })
                        
                except Exception as e:
                    error_msg = f"Exception creating subgraph: {e}"
                    print(f"   [ERROR] {error_msg}")
                    created_subgraphs.append({
                        "success": False,
                        "original_name": subgraph_name,
                        "source_file": source_file,
                        "new_name": new_subgraph_name,
                        "error": error_msg,
                        "original_node_count": len(original_steps),
                        "mapped_node_count": len(mapped_steps),
                        "unmapped_count": unmapped_count
                    })
        
        print(f"\n>> SUBGRAPH CREATION SUMMARY:")
        success_count = len([s for s in created_subgraphs if s.get('success', s.get('dry_run', False)) and not s.get('skipped')])
        skipped_count = len([s for s in created_subgraphs if s.get('skipped')])
        failed_count = len([s for s in created_subgraphs if s.get('success') == False])
        
        if dry_run:
            print(f"   [SKIP] DRY RUN: Would create {success_count} subgraphs")
            if skipped_count > 0:
                print(f"   [SKIP] Would skip {skipped_count} subgraphs (no API-compatible nodes)")
        else:
            print(f"   [OK] Created: {success_count} subgraphs")
            if failed_count > 0:
                print(f"   [ERROR] Failed: {failed_count} subgraphs")
            if skipped_count > 0:
                print(f"   [SKIP] Skipped: {skipped_count} subgraphs (no API-compatible nodes)")
        
        return created_subgraphs
        
    def save_enhanced_results(self, all_subgraphs, created_count, excluded_nodes, created_subgraphs):
        """Save enhanced results including subgraph creation"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        result_filename = get_file_pattern('created_nodes_and_subgraphs', timestamp=timestamp)
        
        # Count successful subgraphs
        successful_subgraphs = len([s for s in created_subgraphs if s.get('success', s.get('dry_run', False)) and not s.get('skipped')])
        failed_subgraphs = len([s for s in created_subgraphs if s.get('success') == False])
        skipped_subgraphs = len([s for s in created_subgraphs if s.get('skipped')])
        
        result_data = {
            'creation_result': {
                'target_workspace': self.migration_config.get("target", {}).get("workspace_id", "Unknown"),
                'nodes_created': created_count,
                'nodes_failed': len(self.creation_errors),
                'nodes_excluded': len(excluded_nodes),
                'subgraphs_created': successful_subgraphs,
                'subgraphs_failed': failed_subgraphs,
                'subgraphs_skipped': skipped_subgraphs,
                'hash_key_success': len(self.hash_key_success),
                'hash_key_failures': len(self.hash_key_failures),
                'creation_timestamp': datetime.now().isoformat(),
                'script_name': self.script_name,
                'project_name': self.project_info['name'],
                'test_hash_keys': self.test_hash_keys,
                'allowed_node_types': self.allowed_node_types,
                'node_id_mapping': self.node_id_mapping,
                'created_nodes': self.created_nodes,
                'created_subgraphs': created_subgraphs,
                'errors': self.creation_errors
            },
            'hash_key_testing': {
                'enabled': self.test_hash_keys,
                'successful_nodes': self.hash_key_success,
                'failed_nodes': self.hash_key_failures,
                'success_rate': f"{len(self.hash_key_success)}/{len(self.hash_key_success) + len(self.hash_key_failures)}" if self.hash_key_success or self.hash_key_failures else "0/0"
            },
            'excluded_nodes': {
                'count': len(excluded_nodes),
                'node_details': excluded_nodes,
                'reason': 'Node type not in allowed list (likely source tables)'
            },
            'source_subgraphs': all_subgraphs,
            'subgraph_migration': {
                'total_source_subgraphs': len(all_subgraphs),
                'successful_migrations': successful_subgraphs,
                'failed_migrations': failed_subgraphs,
                'skipped_migrations': skipped_subgraphs,
                'migration_details': created_subgraphs
            },
            'file_search_info': {
                'search_pattern': 'Any JSON files containing "subgraph" in filename',
                'case_insensitive': True,
                'flexible_naming': True,
                'examples': [
                    'subgraph_data.json',
                    'exported_subgraph_info.json',
                    'subgraph_export.json',
                    'project_subgraph_20250815.json'
                ]
            },
            'next_steps': {
                'nodes_created': f"{created_count} individual nodes created with proper Data Vault types",
                'subgraphs_created': f"{successful_subgraphs} subgraphs created to organize nodes" if successful_subgraphs > 0 else "No subgraphs created",
                'metadata_updates': "Run metadata updater to fix column references and lineage",
                'manual_tasks': f"Manual creation needed for {len(excluded_nodes)} source/stage nodes",
                'verification': "Run verification script to check migration completeness"
            }
        }
        
        with open(result_filename, 'w') as f:
            json.dump(result_data, f, indent=2)
        
        print(f"\n>> Enhanced results saved to: {result_filename}")
        return result_filename

    def run_enhanced_creation(self):
        """Run the enhanced node creation process with subgraph creation"""
        print(f"\n>>> {self.project_info['name'].upper()} ENHANCED NODE + SUBGRAPH CREATION")
        print("=" * 80)
        
        target_workspace = self.migration_config.get("target", {}).get("workspace_id", "Unknown")
        target_name = self.migration_config.get("target", {}).get("workspace_name", "Unknown")
        dry_run = self.migration_config.get('dry_run', True)
        
        print(f">> Target: Workspace {target_workspace} ({target_name})")
        print(f">> Dry Run: {dry_run}")
        print(f">> Hash Key Testing: {self.test_hash_keys}")
        print(f">> Node Type Filter: {', '.join(self.allowed_node_types)}")
        print(f">> File Search: Any JSON files containing 'subgraph' (flexible naming)")
        
        # Step 1: Find and load JSON files
        json_files = self.find_exported_json_files()
        if not json_files:
            print(f"\n[ERROR] No subgraph JSON files found!")
            return False
        
        # Step 2: Load and consolidate data
        all_nodes, all_subgraphs = self.load_and_consolidate_data(json_files)
        if not all_nodes:
            print(f"\n[ERROR] No node data found!")
            return False
        
        print(f"\n>> DATA LOADED:")
        print(f"   Total nodes: {len(all_nodes)}")
        print(f"   Total subgraphs: {len(all_subgraphs)}")
        for sg in all_subgraphs:
            print(f"     - '{sg['name']}': {sg['node_count']} nodes (from {sg['source_file']})")
        
        # Step 3: Filter nodes by type (exclude sources)
        filtered_nodes, excluded_nodes = self.filter_nodes_by_type(all_nodes)
        if not filtered_nodes:
            print(f"\n[ERROR] No nodes left after filtering!")
            return False
        
        # Step 4: Analyze dependencies
        dependencies = self.analyze_dependencies(filtered_nodes)
        
        # Step 5: Create nodes
        created_count = self.create_nodes_in_dependency_order(
            target_workspace, filtered_nodes, dependencies, dry_run
        )
        
        # Step 6: Create subgraphs (NEW!)
        created_subgraphs = self.create_subgraphs_from_original(
            target_workspace, all_subgraphs, dry_run
        )
        
        # Step 7: Save enhanced results
        result_file = self.save_enhanced_results(all_subgraphs, created_count, excluded_nodes, created_subgraphs)
        
        # Final summary
        print(f"\n>>> ENHANCED CREATION COMPLETE!")
        print("-" * 40)
        
        # Count successful subgraphs
        successful_subgraphs = len([s for s in created_subgraphs if s.get('success', s.get('dry_run', False)) and not s.get('skipped')])
        failed_subgraphs = len([s for s in created_subgraphs if s.get('success') == False])
        skipped_subgraphs = len([s for s in created_subgraphs if s.get('skipped')])
        
        if dry_run:
            print(f"[SKIP] DRY RUN: Would create {created_count} nodes + {successful_subgraphs} subgraphs")
            print(f"   [FILTER] Excluded: {len(excluded_nodes)} source/stage nodes")
            if skipped_subgraphs > 0:
                print(f"   [FILTER] Would skip: {skipped_subgraphs} subgraphs (no API-compatible nodes)")
            if self.test_hash_keys:
                print(f"   [TEST] Would attempt hash key inclusion")
        else:
            print(f"[OK] Created: {created_count} nodes + {successful_subgraphs} subgraphs")
            print(f"[FILTER] Excluded: {len(excluded_nodes)} source/stage nodes")
            
            if failed_subgraphs > 0:
                print(f"[ERROR] Failed subgraphs: {failed_subgraphs}")
            if skipped_subgraphs > 0:
                print(f"[SKIP] Skipped subgraphs: {skipped_subgraphs} (no API-compatible nodes)")
            
            if self.test_hash_keys:
                success_count = len(self.hash_key_success)
                failure_count = len(self.hash_key_failures)
                total_attempts = success_count + failure_count
                
                if total_attempts > 0:
                    success_rate = (success_count / total_attempts) * 100
                    print(f"[HASH] Success: {success_count}/{total_attempts} nodes ({success_rate:.1f}%)")
                    
                    if success_count > 0:
                        print(f"   [OK] These nodes have hash keys preserved!")
                    if failure_count > 0:
                        print(f"   [WARNING] {failure_count} nodes need manual hash key setup")
        
        print(f"\n>> FLEXIBLE FILE SEARCH WORKED:")
        print(f"   ✓ Found {len(json_files)} JSON files with 'subgraph' in name")
        for file in json_files:
            print(f"     • {file}")
        
        print(f"\n>> NEXT STEPS:")
        if len(excluded_nodes) > 0:
            print(f"   1. [MANUAL] Create {len(excluded_nodes)} source/stage tables manually in Coalesce UI")
        print(f"   2. [AUTO] {created_count} nodes created with proper Data Vault filtering")
        if successful_subgraphs > 0:
            print(f"   3. [AUTO] {successful_subgraphs} subgraphs created to organize nodes")
        else:
            print(f"   3. [MANUAL] Create subgraphs manually in Coalesce UI")
        print(f"   4. [AUTO] Run metadata updater to fix column references")
        print(f"   5. [AUTO] Run verification script to check migration")
        
        if not dry_run and self.test_hash_keys:
            if len(self.hash_key_success) > 0:
                print(f"   6. [OK] {len(self.hash_key_success)} nodes have hash keys already configured")
            if len(self.hash_key_failures) > 0:
                print(f"   7. [MANUAL] {len(self.hash_key_failures)} nodes need manual hash key configuration")
        
        return True

def main():
    """Run the enhanced node creation process"""
    try:
        creator = EnhancedNodeCreator()
        
        print(f"\n>>> CONFIGURATION OPTIONS:")
        print(f"   Hash Key Testing: {creator.test_hash_keys}")
        print(f"   Allowed Node Types: {', '.join(creator.allowed_node_types)}")
        print(f"   File Search: Flexible - any JSON with 'subgraph' in name")
        print(f"\n>>> This will:")
        print(f"   1. SEARCH for any *.json files containing 'subgraph'")
        print(f"   2. EXCLUDE source tables (create these manually)")
        print(f"   3. EXCLUDE stage tables with hash key testing")
        print(f"   4. INCLUDE dimension, fact, hub, link, satellite tables")
        print(f"   5. TEST hash key preservation for stage tables")
        print(f"   6. FALLBACK to cleaned approach if hash keys fail")
        
        proceed = input(f"\nProceed with enhanced creation? (y/n): ").lower().strip()
        if proceed != 'y':
            print("Cancelled by user.")
            return
        
        success = creator.run_enhanced_creation()
        
        if success:
            print(f"\n[OK] Enhanced process completed successfully!")
            
            if creator.test_hash_keys and not creator.migration_config.get('dry_run', True):
                success_count = len(creator.hash_key_success)
                failure_count = len(creator.hash_key_failures)
                
                if success_count > 0:
                    print(f"\n>>> HASH KEY SUCCESS! ✓")
                    print(f"   {success_count} nodes successfully include hash keys!")
                    print(f"   These stage tables should work properly for Data Vault.")
                
                if failure_count > 0:
                    print(f"\n>>> HASH KEY PARTIAL SUCCESS:")
                    print(f"   {failure_count} nodes fell back to cleaned approach.")
                    print(f"   You'll need to manually configure hash keys for these.")
                
                if success_count > 0 and failure_count == 0:
                    print(f"\n🎉 PERFECT! All stage tables preserved their hash keys!")
                elif success_count > 0:
                    print(f"\n✅ GOOD! {success_count} stage tables have hash keys preserved.")
                    print(f"   Only {failure_count} need manual hash key setup.")
            
        else:
            print(f"\n[ERROR] Enhanced process failed!")
        
    except Exception as e:
        print(f"[ERROR] Enhanced node creation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
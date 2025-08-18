#!/usr/bin/env python3
"""
Universal Node Metadata Updater - POST-CREATION CONFIGURATION
Updates nodes that were created by the node creator script with proper metadata
Uses the PUT /api/v1/workspaces/:workspaceID/nodes/:nodeID endpoint
Fixes the columnReferences mapping and config structure issues
Works with any project - all configuration comes from migration_config.py
Uses flexible naming conventions for file discovery
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
from migration_config import get_migration_config, get_project_info

class UniversalNodeMetadataUpdater:
    """Updates existing nodes with proper metadata configuration for any project"""
    
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
        
        # Track update results
        self.update_successes = []
        self.update_errors = []
        self.failed_updates = []  # For manual action tracking
        
        # Get script name for metadata
        self.script_name = os.path.basename(sys.argv[0]) if sys.argv else "flexible_metadata_updater.py"
        
        print(f">> {self.project_info['name'].upper()} NODE METADATA UPDATER INITIALIZED")
        print(f"   Project: {self.project_info['name']} ({self.project_info['identifier']})")
        print(f"   Script: {self.script_name}")
        print(f"   API: {self.base_url}")
        print(f"   Purpose: Update existing nodes with proper metadata using PUT endpoint")
        print(f"   Searches for: *created_nodes*.json files (flexible naming)")

    def _get_flexible_filename(self, file_type, timestamp=None):
        """Generate flexible filenames without hardcoded prefixes"""
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if file_type == 'metadata_updates':
            return f"metadata_updates_{timestamp}.json"
        elif file_type == 'manual_updates':
            return f"manual_updates_required_{timestamp}.txt"
        else:
            return f"{file_type}_{timestamp}.json"
    
    def find_creation_result_files(self):
        """Find the results from the node creator script - any file with 'created_nodes' anywhere in the name"""
        print(f"\n>>> LOOKING FOR NODE CREATION RESULT FILES")
        print("-" * 50)
        
        # Try patterns for files with "created_nodes" anywhere in the name - flexible naming
        patterns_to_try = [
            # Primary pattern - any file with created_nodes
            "**created_nodes*.json",
            # Alternative patterns
            "*nodes_created*.json",
            "*creation_result*.json",
            "*node_creation*.json",
            "*created*.json",
            # Very broad fallback
            "*.json"
        ]
        
        result_files = []
        
        for i, pattern in enumerate(patterns_to_try):
            print(f">> Trying pattern {i+1}: {pattern}")
            found_files = glob.glob(pattern)
            
            if found_files:
                # Filter for files containing "created_nodes" or related keywords
                if pattern == "*.json":
                    # For the broad pattern, filter for files likely to be creation results
                    found_files = [f for f in found_files if any(keyword in f.lower() for keyword in ['created_nodes', 'nodes_created', 'creation_result', 'node_creation'])]
                    if found_files:
                        print(f"   [OK] Found {len(found_files)} files likely to be creation results")
                    else:
                        print(f"   [INFO] Found JSON files, but none seem to be creation results")
                        continue
                elif pattern == "*created*.json":
                    # For created pattern, prefer files with "nodes" in the name
                    priority_files = [f for f in found_files if 'nodes' in f.lower()]
                    if priority_files:
                        found_files = priority_files
                        print(f"   [OK] Found {len(found_files)} created files with 'nodes' in name")
                    else:
                        print(f"   [OK] Found {len(found_files)} files with 'created' in name")
                else:
                    print(f"   [OK] Found {len(found_files)} files with this pattern")
                
                result_files = found_files
                break
            else:
                print(f"   [INFO] No files found with this pattern")
        
        if not result_files:
            print(f"\n[ERROR] No creation result files found with any pattern!")
            print(f"[INFO] Current directory contents:")
            all_files = glob.glob("*")
            json_files_in_dir = [f for f in all_files if f.endswith('.json')]
            if json_files_in_dir:
                print(f"   Available JSON files:")
                for file in json_files_in_dir:
                    print(f"      - {file}")
                print(f"\n[SUGGESTION] Run the node creator script first to generate files with 'created_nodes' in the name")
            else:
                print(f"   No JSON files found in current directory")
                print(f"   Expected format: *created_nodes*.json")
            return []
        
        # Sort by modification time (newest first)
        result_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        print(f"\n[SUCCESS] Found {len(result_files)} creation result files:")
        for i, file in enumerate(result_files):
            file_path = Path(file)
            file_size = file_path.stat().st_size / 1024  # KB
            mod_time = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            print(f"   {i+1}. {file} ({file_size:.1f} KB, {mod_time})")
        
        print(f"\n>> Flexible naming support:")
        print(f"   ✓ project_created_nodes_20250815.json")
        print(f"   ✓ data_created_nodes.json") 
        print(f"   ✓ created_nodes_backup.json")
        print(f"   ✓ my_created_nodes_results.json")
        print(f"   ✓ *created_nodes*.json (any pattern)")
        
        return result_files

    def find_original_json_files(self):
        """Find the original exported subgraph JSON files using flexible patterns"""
        print(f"\n>>> LOOKING FOR ORIGINAL SUBGRAPH EXPORT FILES")
        print("-" * 50)
        
        # Try patterns in order of preference - prioritize EXPORT files (not migration results)
        patterns_to_try = [
            # Primary: export files (subgraph_* but NOT subgraph_migration_*)
            "subgraph_*.json",
            # Secondary: flexible subgraph patterns  
            "*_subgraph_*.json",
            "*subgraph*.json",
            # Broader patterns
            "*exported*.json", 
            "*export*.json",
            "*original*.json",
            # Very broad fallback
            "*.json"
        ]
        
        json_files = []
        
        for i, pattern in enumerate(patterns_to_try):
            print(f">> Trying pattern {i+1}: {pattern}")
            found_files = glob.glob(pattern)
            
            if found_files:
                # For subgraph_*.json, filter OUT migration results
                if pattern == "subgraph_*.json":
                    # Exclude subgraph_migration_* files (those are migration results, not source data)
                    found_files = [f for f in found_files if not f.startswith('subgraph_migration_')]
                    if found_files:
                        print(f"   [OK] Found {len(found_files)} EXPORT files (excluding migration results)")
                    else:
                        print(f"   [INFO] Found subgraph files, but all were migration results")
                        continue
                # If using broad pattern, filter for subgraph files
                elif pattern == "*.json":
                    found_files = [f for f in found_files if any(keyword in f.lower() for keyword in ['subgraph', 'exported', 'export', 'original', 'migration']) and not any(exclude in f.lower() for exclude in ['created', 'update', 'result', 'manual'])]
                    if found_files:
                        print(f"   [OK] Found {len(found_files)} files likely to be subgraph exports")
                    else:
                        print(f"   [INFO] Found JSON files, but none seem to be subgraph exports")
                        continue
                else:
                    print(f"   [OK] Found {len(found_files)} files with this pattern")
                
                json_files = found_files
                break
            else:
                print(f"   [INFO] No files found with this pattern")
        
        if not json_files:
            print(f"\n[ERROR] No original EXPORT files found with any pattern!")
            print(f"[INFO] Current directory contents:")
            all_files = glob.glob("*")
            json_files_in_dir = [f for f in all_files if f.endswith('.json')]
            if json_files_in_dir:
                print(f"   Available JSON files:")
                for file in json_files_in_dir:
                    print(f"      - {file}")
                print(f"\n[SUGGESTION] Run the subgraph export script first to generate EXPORT files")
                print(f"   NEED: subgraph_*.json (export files)")
                print(f"   NOT: subgraph_migration_*.json (migration results)")
            else:
                print(f"   No JSON files found in current directory")
            return []
        
        print(f"\n[SUCCESS] Found {len(json_files)} original subgraph EXPORT files:")
        for i, file in enumerate(json_files):
            file_path = Path(file)
            file_size = file_path.stat().st_size / 1024  # KB
            print(f"   {i+1}. {file} ({file_size:.1f} KB)")
        
        print(f"\n>> EXPORT file pattern priority:")
        print(f"   ✓ subgraph_*.json (PRIMARY - export files with original node data)")
        print(f"   ✓ *_subgraph_*.json (flexible patterns)")
        print(f"   ✓ *subgraph*.json (fallback pattern)")
        print(f"   ❌ Excludes subgraph_migration_* (migration results, not source data)")
        print(f"   ✓ No hardcoded project names")
        
        return json_files

    def load_creation_results(self, result_file):
        """Load the node creation results with ID mappings"""
        print(f"\n📂 LOADING NODE CREATION RESULTS")
        print(f"File: {result_file}")
        print("-" * 50)
        
        try:
            with open(result_file, 'r') as f:
                data = json.load(f)
            
            creation_result = data.get('creation_result', {})
            node_id_mapping = creation_result.get('node_id_mapping', {})
            created_nodes = creation_result.get('created_nodes', [])
            
            print(f"[OK] Loaded creation results:")
            print(f"   * Node ID mappings: {len(node_id_mapping)}")
            print(f"   * Created nodes: {len(created_nodes)}")
            print(f"   * Target workspace: {creation_result.get('target_workspace', 'Unknown')}")
            print(f"   * Project: {creation_result.get('project_name', 'Unknown')}")
            
            return {
                'node_id_mapping': node_id_mapping,
                'created_nodes': created_nodes,
                'target_workspace': creation_result.get('target_workspace'),
                'full_data': data
            }
            
        except Exception as e:
            print(f"[ERROR] Error loading creation results: {e}")
            return None
    
    def load_original_node_data(self, json_files):
        """Load all original node data from exported JSON files"""
        print(f"\n📂 LOADING ORIGINAL NODE DATA")
        print("-" * 50)
        
        all_nodes = {}
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                subgraph_name = data.get('subgraph_name', 'Unknown')
                node_details = data.get('node_details', {})
                
                print(f">> Loading: '{subgraph_name}' ({len(node_details)} nodes)")
                
                # Add nodes to consolidated collection
                for node_id, node_data in node_details.items():
                    if node_id not in all_nodes:
                        all_nodes[node_id] = node_data
                    else:
                        print(f"   [WARNING] Duplicate node ID found: {node_id}")
                
            except Exception as e:
                print(f"[ERROR] Error loading {json_file}: {e}")
                continue
        
        print(f"\n>> Original data loaded: {len(all_nodes)} unique nodes")
        return all_nodes
    
    def build_proper_metadata(self, original_node_data, node_id_mapping):
        """Build properly formatted metadata with mapped columnReferences"""
        original_metadata = original_node_data.get('metadata', {})
        original_columns = original_metadata.get('columns', [])
        original_source_mapping = original_metadata.get('sourceMapping', [])
        
        if not original_columns:
            return None
        
        # Build proper columns with mapped columnReferences
        proper_columns = []
        
        for col in original_columns:
            proper_col = {
                'name': col.get('name', 'UNKNOWN_COLUMN'),
                'dataType': col.get('dataType', 'VARCHAR'),
                'nullable': col.get('nullable', True),
                'description': col.get('description', '')
            }
            
            # Add columnID if present
            if 'columnID' in col:
                proper_col['columnID'] = str(col['columnID'])
            
            # Add defaultValue if present and simple
            if 'defaultValue' in col and col['defaultValue'] and isinstance(col['defaultValue'], (str, int, float, bool)):
                proper_col['defaultValue'] = str(col['defaultValue'])
            
            # Add primaryKey if present
            if 'primaryKey' in col and isinstance(col['primaryKey'], bool):
                proper_col['primaryKey'] = col['primaryKey']
            
            # Build proper sources with mapped columnReferences
            original_sources = col.get('sources', [])
            if original_sources:
                proper_sources = []
                
                for source in original_sources:
                    proper_source = {
                        'transform': source.get('transform', ''),
                        'columnReferences': []
                    }
                    
                    # Map columnReferences to new node IDs
                    original_col_refs = source.get('columnReferences', [])
                    for col_ref in original_col_refs:
                        original_node_id = col_ref.get('nodeID')
                        column_id = col_ref.get('columnID')
                        
                        if original_node_id and column_id:
                            # Map to new node ID if available
                            new_node_id = node_id_mapping.get(original_node_id, original_node_id)
                            
                            proper_source['columnReferences'].append({
                                'nodeID': new_node_id,
                                'columnID': column_id
                            })
                    
                    proper_sources.append(proper_source)
                
                proper_col['sources'] = proper_sources
            
            # Add config if present
            if 'config' in col and isinstance(col['config'], dict):
                proper_col['config'] = col['config']
            
            proper_columns.append(proper_col)
        
        # Build proper sourceMapping with mapped node IDs
        proper_source_mapping = []
        for source_map in original_source_mapping:
            aliases = source_map.get('aliases', {})
            dependencies = source_map.get('dependencies', [])
            
            # Map aliases to new node IDs
            mapped_aliases = {}
            for alias_name, original_node_id in aliases.items():
                new_node_id = node_id_mapping.get(original_node_id, original_node_id)
                mapped_aliases[alias_name] = new_node_id
            
            proper_source_map = {
                'aliases': mapped_aliases,
                'customSQL': source_map.get('customSQL', {'customSQL': ''}),
                'dependencies': dependencies,  # Keep dependency names as-is
                'join': source_map.get('join', {}),
                'name': source_map.get('name', 'default'),
                'noLinkRefs': source_map.get('noLinkRefs', [])
            }
            
            proper_source_mapping.append(proper_source_map)
        
        # Build complete metadata
        proper_metadata = {
            'columns': proper_columns
        }
        
        if proper_source_mapping:
            proper_metadata['sourceMapping'] = proper_source_mapping
        
        # Add other metadata fields if present
        if 'cteString' in original_metadata:
            proper_metadata['cteString'] = original_metadata['cteString']
        
        if 'appliedNodeTests' in original_metadata:
            proper_metadata['appliedNodeTests'] = original_metadata['appliedNodeTests']
        
        if 'enabledColumnTestIDs' in original_metadata:
            proper_metadata['enabledColumnTestIDs'] = original_metadata['enabledColumnTestIDs']
        
        return proper_metadata
    
    def update_node_metadata(self, workspace_id, new_node_id, original_node_data, node_id_mapping, dry_run=True):
        """Update a single node's metadata using PUT endpoint"""
        node_name = original_node_data.get('name', f'Node_{new_node_id}')
        
        if dry_run:
            print(f"   [SKIP] DRY RUN: Would update metadata for '{node_name}' (ID: {new_node_id})")
            return True
        
        try:
            # Build proper metadata
            proper_metadata = self.build_proper_metadata(original_node_data, node_id_mapping)
            
            if not proper_metadata:
                print(f"   [WARNING] No metadata to update for '{node_name}'")
                return True
            
            # Build the complete PUT payload with ALL required fields
            update_payload = {
                'id': new_node_id,
                'name': node_name,
                'nodeType': original_node_data.get('nodeType', 'Stage'),
                'locationName': original_node_data.get('locationName', ''),
                'database': original_node_data.get('database', ''),
                'schema': original_node_data.get('schema', ''),
                'table': original_node_data.get('table', node_name),  # REQUIRED: Use table name or node name
                'description': original_node_data.get('description', f'Updated {self.project_info["name"]} node: {node_name}'),
                'isMultisource': original_node_data.get('isMultisource', False),  # REQUIRED: Default to False
                'materializationType': original_node_data.get('materializationType', 'table'),  # Usually 'table' or 'view'
                'overrideSQL': original_node_data.get('overrideSQL', False),  # Usually False
                'metadata': proper_metadata,
                'config': original_node_data.get('config', {
                    'insertStrategy': 'INSERT',
                    'postSQL': '',
                    'preSQL': '',
                    'testsEnabled': True
                })
            }
            
            # Log payload info
            column_count = len(proper_metadata.get('columns', []))
            source_mapping_count = len(proper_metadata.get('sourceMapping', []))
            payload_size = len(json.dumps(update_payload))
            
            print(f"   >> Updating: {column_count} columns, {source_mapping_count} source mappings")
            print(f"   >> Payload size: {payload_size} chars")
            
            # Make the PUT request
            response = requests.put(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/nodes/{new_node_id}",
                headers=self.headers,
                json=update_payload,
                timeout=60
            )
            
            print(f"   📡 Status: {response.status_code}")
            
            if response.status_code in [200, 201]:
                print(f"   [OK] SUCCESS: '{node_name}' metadata updated")
                self.update_successes.append({
                    'node_id': new_node_id,
                    'name': node_name,
                    'column_count': column_count,
                    'source_mapping_count': source_mapping_count
                })
                return True
            else:
                error_msg = f"Update failed: {response.status_code}"
                try:
                    error_detail = response.json() if response.text else response.text[:200]
                    error_msg += f" - {error_detail}"
                except:
                    error_msg += f" - {response.text[:200]}"
                
                print(f"   [ERROR] FAILED: {error_msg}")
                self.update_errors.append({
                    'node_id': new_node_id,
                    'name': node_name,
                    'error': error_msg,
                    'status_code': response.status_code
                })
                # Track for manual action
                self.failed_updates.append({
                    'node_id': new_node_id,
                    'name': node_name,
                    'error': error_msg,
                    'status_code': response.status_code,
                    'action_required': 'Manual metadata update in Coalesce UI',
                    'has_columns': len(proper_metadata.get('columns', [])) > 0,
                    'has_source_mapping': len(proper_metadata.get('sourceMapping', [])) > 0
                })
                return False
                
        except requests.exceptions.Timeout:
            error_msg = f"Timeout updating '{node_name}'"
            print(f"   [ERROR] {error_msg}")
            self.update_errors.append({
                'node_id': new_node_id,
                'name': node_name,
                'error': error_msg,
                'status_code': 'timeout'
            })
            # Track for manual action
            self.failed_updates.append({
                'node_id': new_node_id,
                'name': node_name,
                'error': error_msg,
                'status_code': 'timeout',
                'action_required': 'Manual metadata update in Coalesce UI (timeout)',
                'has_columns': proper_metadata and len(proper_metadata.get('columns', [])) > 0,
                'has_source_mapping': proper_metadata and len(proper_metadata.get('sourceMapping', [])) > 0
            })
            return False
        except Exception as e:
            error_msg = f"Exception updating '{node_name}': {e}"
            print(f"   [ERROR] {error_msg}")
            self.update_errors.append({
                'node_id': new_node_id,
                'name': node_name,
                'error': error_msg,
                'status_code': 'exception'
            })
            # Track for manual action
            self.failed_updates.append({
                'node_id': new_node_id,
                'name': node_name,
                'error': error_msg,
                'status_code': 'exception',
                'action_required': 'Manual metadata update in Coalesce UI (exception)',
                'has_columns': proper_metadata and len(proper_metadata.get('columns', [])) > 0,
                'has_source_mapping': proper_metadata and len(proper_metadata.get('sourceMapping', [])) > 0
            })
            return False
    
    def update_all_nodes(self, workspace_id, creation_results, original_nodes, dry_run=True):
        """Update metadata for all created nodes"""
        print(f"\n>> UPDATING NODE METADATA")
        print(f"Workspace: {workspace_id}")
        print(f"Project: {self.project_info['name']}")
        print(f"Dry run: {dry_run}")
        print("-" * 60)
        
        node_id_mapping = creation_results['node_id_mapping']
        created_nodes = creation_results['created_nodes']
        
        print(f">> Update plan:")
        print(f"   * Total nodes to update: {len(created_nodes)}")
        print(f"   * Node ID mappings available: {len(node_id_mapping)}")
        
        updated_count = 0
        
        for i, created_node in enumerate(created_nodes, 1):
            original_id = created_node['original_id']
            new_node_id = created_node['new_id']
            node_name = created_node['name']
            
            print(f"\n>> {i}/{len(created_nodes)}: Updating '{node_name}'")
            print(f"   🆔 {original_id} -> {new_node_id}")
            
            # Get original node data
            original_node_data = original_nodes.get(original_id)
            if not original_node_data:
                print(f"   [ERROR] Original node data not found for {original_id}")
                self.update_errors.append({
                    'node_id': new_node_id,
                    'name': node_name,
                    'error': 'Original node data not found',
                    'status_code': 'missing_data'
                })
                # Track for manual action
                self.failed_updates.append({
                    'node_id': new_node_id,
                    'name': node_name,
                    'error': 'Original node data not found',
                    'status_code': 'missing_data',
                    'action_required': 'Manual metadata configuration in Coalesce UI (no source data)',
                    'has_columns': False,
                    'has_source_mapping': False
                })
                continue
            
            # Update the node
            success = self.update_node_metadata(
                workspace_id, new_node_id, original_node_data, node_id_mapping, dry_run
            )
            
            if success:
                updated_count += 1
        
        print(f"\n>> UPDATE SUMMARY:")
        if dry_run:
            print(f"   [SKIP] DRY RUN: Would update {updated_count} nodes")
        else:
            print(f"   [OK] Successfully updated: {len(self.update_successes)} nodes")
            print(f"   [ERROR] Failed updates: {len(self.update_errors)} nodes")
            
            if self.update_errors:
                # Group errors by status code
                error_codes = {}
                for error in self.update_errors:
                    code = error.get('status_code', 'unknown')
                    if code not in error_codes:
                        error_codes[code] = 0
                    error_codes[code] += 1
                
                print(f"   >> Error breakdown:")
                for code, count in error_codes.items():
                    print(f"      * {code}: {count} errors")
        
        return updated_count
    
    def save_update_results(self, creation_results, updated_count):
        """Save update results using flexible naming"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        result_filename = self._get_flexible_filename('metadata_updates', timestamp)
        
        result_data = {
            'update_result': {
                'target_workspace': creation_results['target_workspace'],
                'nodes_updated': updated_count,
                'nodes_failed': len(self.update_errors),
                'total_nodes_attempted': len(creation_results['created_nodes']),
                'update_timestamp': datetime.now().isoformat(),
                'script_name': self.script_name,
                'api_base_url': self.base_url,
                'project_name': self.project_info['name'],
                'project_identifier': self.project_info['identifier'],
                'successful_updates': self.update_successes,
                'failed_updates': self.update_errors
            },
            'original_creation_data': creation_results['full_data'],
            'update_notes': {
                'purpose': 'Update node metadata with proper columnReferences and config',
                'endpoint_used': 'PUT /api/v1/workspaces/:workspaceID/nodes/:nodeID',
                'column_references': 'Mapped to new node IDs from creation results',
                'source_mapping': 'Updated with new node ID aliases',
                'config': 'Added required config structure',
                'project': self.project_info['name'],
                'file_format': 'Flexible naming convention'
            }
        }
        
        with open(result_filename, 'w') as f:
            json.dump(result_data, f, indent=2)
        
        print(f"\n>> Update results saved to: {result_filename}")
        print(f"   >> Project: {self.project_info['name']}")
        return result_filename
    
    def save_failed_updates(self):
        """Save list of nodes that couldn't be updated for manual action using flexible naming"""
        if not self.failed_updates:
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self._get_flexible_filename('manual_updates', timestamp)
        
        with open(filename, 'w') as f:
            f.write(f"[ALERT] MANUAL UPDATE REQUIRED - {self.project_info['name'].upper()}\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Project: {self.project_info['name']} ({self.project_info['identifier']})\n")
            f.write(f"Script: {self.script_name}\n\n")
            
            f.write(f"[WARNING] {len(self.failed_updates)} NODE METADATA UPDATES FAILED\n")
            f.write("-" * 80 + "\n\n")
            
            f.write("ACTION REQUIRED: Manually update node metadata in Coalesce UI\n\n")
            
            for i, failed in enumerate(self.failed_updates, 1):
                f.write(f"{i:3d}. Node Name: {failed['name']}\n")
                f.write(f"     Node ID: {failed['node_id']}\n")
                f.write(f"     Error: {failed['error']}\n")
                f.write(f"     Status: {failed['status_code']}\n")
                f.write(f"     Has Columns: {'Yes' if failed.get('has_columns') else 'No'}\n")
                f.write(f"     Has Source Mapping: {'Yes' if failed.get('has_source_mapping') else 'No'}\n")
                f.write(f"     Action: {failed['action_required']}\n")
                f.write(f"     Steps: 1) Open Coalesce UI\n")
                f.write(f"            2) Navigate to node {failed['node_id']}\n")
                f.write(f"            3) Configure node metadata manually\n")
                f.write(f"            4) Set up column transformations\n")
                f.write(f"            5) Configure source mappings\n")
                f.write(f"            6) Test node execution\n\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write(">> NEXT STEPS:\n")
            f.write("1. Review each failed node update\n")
            f.write("2. Manually configure metadata in Coalesce UI\n")
            f.write("3. Test node executions to verify functionality\n")
            f.write("4. Update this list as nodes are manually configured\n")
            f.write("5. Document any patterns in failures for future migrations\n")
        
        print(f"\n>> Manual action file created: {filename}")
        print(f"   [WARNING] {len(self.failed_updates)} nodes require manual metadata update")
        return filename

    def run_metadata_updates(self):
        """Run the complete metadata update process"""
        print(f"\n>>> {self.project_info['name'].upper()} NODE METADATA UPDATE PROCESS")
        print("=" * 80)
        
        # Get configuration
        dry_run = self.migration_config.get('dry_run', True)
        
        print(f">> Dry Run: {dry_run}")
        print(f">> Script: {self.script_name}")
        print(f">> API: {self.base_url}")
        print(f">> Project: {self.project_info['name']} ({self.project_info['identifier']})")
        print(f">>> Purpose: Update existing nodes with proper metadata")
        print(f">>> Searches for: *created_nodes*.json files (flexible naming)")
        
        # Step 1: Find creation result files
        result_files = self.find_creation_result_files()
        if not result_files:
            print(f"\n[ERROR] No creation result files found!")
            print(f">> Run the node creator script first")
            return False
        
        # Use the most recent result file
        latest_result_file = result_files[0]
        print(f"\n>> Using latest result file: {latest_result_file}")
        
        # Step 2: Load creation results
        creation_results = self.load_creation_results(latest_result_file)
        if not creation_results:
            print(f"\n[ERROR] Could not load creation results!")
            return False
        
        # Step 3: Find and load original JSON files
        json_files = self.find_original_json_files()
        if not json_files:
            print(f"\n[ERROR] No original JSON files found!")
            print(f">> Need the original exported subgraph JSON files")
            return False
        
        original_nodes = self.load_original_node_data(json_files)
        if not original_nodes:
            print(f"\n[ERROR] No original node data found!")
            return False
        
        # Step 4: Update all nodes
        workspace_id = creation_results['target_workspace']
        updated_count = self.update_all_nodes(workspace_id, creation_results, original_nodes, dry_run)
        
        # Step 5: Save results
        result_file = self.save_update_results(creation_results, updated_count)
        manual_file = self.save_failed_updates()  # Save failed updates list
        
        # Final summary
        print(f"\n>>> METADATA UPDATE COMPLETE!")
        print("-" * 30)
        
        if dry_run:
            print(f"[SKIP] DRY RUN: Would update {updated_count} nodes")
            if self.failed_updates:
                print(f"[WARNING] Would fail to update: {len(self.failed_updates)} nodes")
                if manual_file:
                    print(f">> Manual action list: {manual_file}")
            print(f"\n>> To actually update metadata:")
            print(f"   1. Edit migration_config.py")
            print(f"   2. Change: 'dry_run': True -> 'dry_run': False")
            print(f"   3. Run: python {self.script_name}")
            print(f"\n>> THIS WILL:")
            print(f"   [OK] Fix columnReferences with proper node ID mappings")
            print(f"   [OK] Add required config structure")
            print(f"   [OK] Update sourceMapping with new node IDs")
            print(f"   [OK] Preserve all column names, types, and transformations")
        else:
            print(f"[OK] Updated: {len(self.update_successes)} nodes")
            if self.update_errors:
                print(f"[ERROR] Errors: {len(self.update_errors)} nodes had issues")
                print(f"   Check {result_file} for error details")
            if self.failed_updates:
                print(f"[WARNING] Manual updates needed: {len(self.failed_updates)} nodes")
                if manual_file:
                    print(f">> Manual action list: {manual_file}")
            
            print(f"\n>>> NEXT STEPS:")
            print(f"   1. Verify nodes in Coalesce UI for workspace {workspace_id}")
            if manual_file:
                print(f"   2. Review {manual_file} for nodes requiring manual metadata updates")
                print(f"   3. Configure metadata manually for failed nodes")
                print(f"   4. Create subgraphs manually using the UI")
            else:
                print(f"   2. Create subgraphs manually using the UI")
            print(f"   3. Test node executions to verify transformations work")
            print(f"   4. Use the original creation result file for subgraph organization")
            print(f"\n>> METADATA UPDATE COMPLETE:")
            print(f"   [OK] Nodes should now have proper columnReferences")
            print(f"   [OK] Required config structure added")
            print(f"   [OK] sourceMapping updated with new node IDs")
            print(f"   >> Project: {self.project_info['name']}")
            print(f"   >> Files use flexible naming convention")
        
        return True

def main():
    """Run the metadata update process"""
    try:
        updater = UniversalNodeMetadataUpdater()
        success = updater.run_metadata_updates()
        
        if success:
            print(f"\n[OK] Process completed successfully!")
            print(f"\n>> This script fixes the metadata issues from the node creator.")
            print(f"   Nodes should now have proper transformations and lineage.")
        else:
            print(f"\n[ERROR] Process failed!")
        
    except Exception as e:
        print(f"[ERROR] Metadata update failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
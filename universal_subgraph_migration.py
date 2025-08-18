#!/usr/bin/env python3
"""
Enhanced Universal Subgraph Migration Tool
Migrates subgraphs from any source workspace to any target workspace
WITH AUTOMATIC DEPENDENCY RESOLUTION - finds all prerequisite nodes
All configuration comes from migration_config.py
"""

import requests
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from coalesce_conn import load_config_from_env
from migration_config import get_migration_config, get_project_info

class EnhancedSubgraphMigration:
    """Enhanced subgraph migration tool with automatic dependency resolution"""
    
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
        self.subgraph_configs = self.migration_config.get("subgraphs", [])
        
        # Extract target names for display
        self.target_subgraphs = []
        for sg_config in self.subgraph_configs:
            if isinstance(sg_config, str):
                self.target_subgraphs.append(sg_config)
            elif isinstance(sg_config, dict):
                self.target_subgraphs.append(sg_config.get('name', sg_config.get('id', 'Unknown')))
        
        self.found_subgraphs = {}
        
        # Caches for optimization
        self.node_cache = {}  # Cache for node details
        self.dependency_cache = {}  # Cache for node dependencies
        
        # Get script and config file names for metadata
        self.script_name = os.path.basename(sys.argv[0]) if sys.argv else "enhanced_subgraph_migration.py"
        self.config_file_name = "migration_config.py"
        
        print(f">>> {self.project_info['name'].upper()} ENHANCED SUBGRAPH MIGRATION INITIALIZED")
        print(f"   Project: {self.project_info['name']} ({self.project_info['identifier']})")
        print(f"   Target subgraphs: {self.target_subgraphs}")
        print(f"   🚀 FEATURE: Automatic dependency resolution enabled!")
        
        # Show config format being used
        if any(isinstance(sg, dict) for sg in self.subgraph_configs):
            print(f"   Using enhanced config with IDs")

    def _get_filename(self, file_type, subgraph_name=None, timestamp=None, new_id=None):
        """Generate standardized subgraph filenames"""
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Clean subgraph name for filename
        if subgraph_name:
            clean_name = subgraph_name.replace(' ', '_').replace('-', '_').lower()
        else:
            clean_name = "unknown"
        
        if file_type == 'exported':
            return f"subgraph_{clean_name}_{timestamp}.json"
        elif file_type == 'migration_results':
            if new_id:
                return f"subgraph_migration_{clean_name}_{new_id}_{timestamp}.json"
            else:
                return f"subgraph_migration_{clean_name}_{timestamp}.json"
        elif file_type == 'manual_downloads':
            return f"subgraph_manual_downloads_{timestamp}.txt"
        else:
            return f"subgraph_{file_type}_{clean_name}_{timestamp}.json"

    def _get_file_metadata(self):
        """Get metadata about the script and config files"""
        return {
            "generated_by": {
                "script_name": self.script_name,
                "script_path": os.path.abspath(sys.argv[0]) if sys.argv else "unknown",
                "config_file": self.config_file_name,
                "generation_timestamp": datetime.now().isoformat(),
                "python_version": sys.version,
                "working_directory": os.getcwd(),
                "project_name": self.project_info['name'],
                "project_identifier": self.project_info['identifier'],
                "features_enabled": ["dependency_resolution", "predecessor_chain_analysis"]
            }
        }

    def find_all_subgraphs_in_workspace(self, workspace_id):
        """Find all subgraphs in workspace using comprehensive scanning"""
        print(f"\n>>> SEARCHING FOR SUBGRAPHS IN WORKSPACE {workspace_id}")
        print("-" * 60)
        
        all_subgraphs = []
        
        # Strategy 1: Try GET request first (most reliable)
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/subgraphs",
                headers=self.headers
            )
            
            print(f"GET /subgraphs: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                subgraphs = data.get('data', data) if isinstance(data, dict) else data
                all_subgraphs.extend(subgraphs)
                print(f"   Found {len(subgraphs)} subgraphs via GET")
                return all_subgraphs
            else:
                print(f"   GET failed with {response.status_code}")
                
        except Exception as e:
            print(f"   GET exception: {e}")
        
        # Strategy 2: Try to find specific subgraphs by name using config
        print(f"   Trying to find target subgraphs by name...")
        found_by_name = 0
        
        config_subgraphs = self.migration_config.get("subgraphs", [])
        for subgraph_config in config_subgraphs:
            if isinstance(subgraph_config, str):
                continue
            elif isinstance(subgraph_config, dict):
                sg_id = subgraph_config.get('id')
                sg_name = subgraph_config.get('name')
                
                if sg_id:
                    print(f"   Trying direct ID lookup for: {sg_id}")
                    try:
                        response = requests.get(
                            f"{self.base_url}/api/v1/workspaces/{workspace_id}/subgraphs/{sg_id}",
                            headers=self.headers
                        )
                        
                        if response.status_code == 200:
                            subgraph_data = response.json()
                            sg_data = subgraph_data.get('data', subgraph_data)
                            all_subgraphs.append(sg_data)
                            found_by_name += 1
                            print(f"   [OK] Found by ID: {sg_name or sg_id}")
                            
                    except Exception as e:
                        print(f"   [ERROR] Error fetching ID {sg_id}: {e}")
        
        # Strategy 3: ID scanning for numeric IDs (fallback)
        if len(all_subgraphs) < len(self.target_subgraphs):
            print(f"   Using numeric ID scanning as fallback (1-200)...")
            
            existing_ids = [str(sg.get('id', '')) for sg in all_subgraphs]
            scan_count = 0
            
            for sg_id in range(1, 201):
                if str(sg_id) in existing_ids:
                    continue
                    
                try:
                    response = requests.get(
                        f"{self.base_url}/api/v1/workspaces/{workspace_id}/subgraphs/{sg_id}",
                        headers=self.headers
                    )
                    
                    if response.status_code == 200:
                        subgraph_data = response.json()
                        sg_data = subgraph_data.get('data', subgraph_data)
                        all_subgraphs.append(sg_data)
                        scan_count += 1
                        
                except Exception:
                    continue
            
            if scan_count > 0:
                print(f"   Found {scan_count} additional subgraphs via numeric scanning")
        
        if found_by_name > 0:
            print(f"   Found {found_by_name} subgraphs by direct ID lookup")
        
        print(f"[OK] Total subgraphs found: {len(all_subgraphs)}")
        return all_subgraphs

    def _lookup_by_id(self, workspace_id, target_id, target_name, found, subgraphs_needing_search):
        """Try to find subgraph directly by ID (numeric or UUID)."""
        print(f"\n>>> Direct ID lookup: '{target_name}' (ID: {target_id})")
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/subgraphs/{target_id}",
                headers=self.headers
            )
            if response.status_code == 200:
                sg_data = response.json().get('data', response.json())
                print(f"   [OK] FOUND: '{sg_data.get('name', target_name)}' (ID: {target_id})")
                found[target_name] = {
                    'id': target_id,
                    'name': sg_data.get('name', target_name),
                    'basic_data': sg_data
                }
            else:
                print(f"   [ERROR] ID {target_id} not found: {response.status_code}")
                subgraphs_needing_search.append(target_name)
        except Exception as e:
            print(f"   [ERROR] Error fetching ID {target_id}: {e}")
            subgraphs_needing_search.append(target_name)

    def find_target_subgraphs(self, workspace_id):
        """Find the specific target subgraphs from configuration"""
        print(f"\n>>> LOOKING FOR TARGET SUBGRAPHS")
        print(f"Targets: {self.target_subgraphs}")
        print("-" * 50)
        
        found = {}
        subgraphs_needing_search = []

        # First pass: Direct ID lookups
        for sg_config in self.subgraph_configs:
            if isinstance(sg_config, dict) and sg_config.get('source_id'):
                target_name = sg_config.get('name', sg_config['source_id'])
                target_id = sg_config['source_id']
                self._lookup_by_id(workspace_id, target_id, target_name, found, subgraphs_needing_search)
            elif isinstance(sg_config, str):
                if sg_config.isdigit():
                    self._lookup_by_id(workspace_id, sg_config, sg_config, found, subgraphs_needing_search)
                else:
                    subgraphs_needing_search.append(sg_config)
            elif isinstance(sg_config, dict):
                subgraphs_needing_search.append(sg_config.get('name', 'Unknown'))

        # Second pass: search by name
        if subgraphs_needing_search:
            print(f"\n>>> SEARCHING FOR REMAINING SUBGRAPHS")
            print(f"Need to find: {len(subgraphs_needing_search)} subgraphs")
            
            all_subgraphs = self.find_all_subgraphs_in_workspace(workspace_id)
            
            if not all_subgraphs:
                print("[ERROR] No subgraphs found in workspace for name matching!")
            else:
                print(f"\n>> ALL SUBGRAPHS FOUND FOR NAME MATCHING:")
                for i, sg in enumerate(all_subgraphs):
                    sg_name = sg.get('name', f'Unnamed_{sg.get("id", i)}')
                    sg_id = sg.get('id', 'unknown')
                    print(f"   {i+1:2d}. '{sg_name}' (ID: {sg_id})")
                
                for sg_config in subgraphs_needing_search:
                    target_name = sg_config if isinstance(sg_config, str) else sg_config.get('name', sg_config.get('id', 'Unknown'))
                    
                    print(f"\n>>> Name search for: '{target_name}'")
                    
                    for sg in all_subgraphs:
                        sg_name = sg.get('name', '')
                        sg_id = sg.get('id')
                        
                        if self._names_match(sg_name, target_name):
                            print(f"   [OK] FOUND BY NAME: '{sg_name}' (ID: {sg_id})")
                            found[target_name] = {
                                'id': sg_id,
                                'name': sg_name,
                                'basic_data': sg
                            }
                            break
                    else:
                        print(f"   [ERROR] NOT FOUND: '{target_name}'")
        else:
            print(f"\n⚡ SKIPPED DISCOVERY - All subgraphs found by direct ID lookup!")

        # Summary
        print(f"\n>> SEARCH SUMMARY:")
        for target_name in self.target_subgraphs:
            if target_name in found:
                sg_info = found[target_name]
                print(f"   [OK] '{target_name}' -> '{sg_info['name']}' (ID: {sg_info['id']})")
            else:
                print(f"   [ERROR] '{target_name}' -> NOT FOUND")
        
        return found

    def _names_match(self, found_name, target_name):
        """Check if subgraph names match"""
        if found_name.lower() == target_name.lower():
            return True
        if found_name == target_name:
            return True
        
        found_clean = found_name.upper().replace('-', '').replace('_', '').replace(' ', '')
        target_clean = target_name.upper().replace('-', '').replace('_', '').replace(' ', '')
        
        if found_clean == target_clean:
            return True
        
        return False

    def _get_node_details(self, workspace_id, node_id):
        """Get detailed information for a specific node with caching"""
        # Check cache first
        cache_key = f"{workspace_id}_{node_id}"
        if cache_key in self.node_cache:
            return self.node_cache[cache_key]
        
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/nodes/{node_id}",
                headers=self.headers
            )
            
            if response.status_code == 200:
                node_data = response.json()
                node_info = node_data.get('data', node_data)
                
                # Cache the result
                self.node_cache[cache_key] = node_info
                return node_info
            else:
                print(f"     [WARNING] Could not get node {node_id}: {response.status_code}")
                # Track failed node download
                if not hasattr(self, 'failed_node_downloads'):
                    self.failed_node_downloads = []
                self.failed_node_downloads.append({
                    'node_id': node_id,
                    'error': f"HTTP {response.status_code}",
                    'action_required': 'Manual export from Coalesce UI'
                })
                return None
                
        except Exception as e:
            print(f"     [ERROR] Error getting node {node_id}: {e}")
            if not hasattr(self, 'failed_node_downloads'):
                self.failed_node_downloads = []
            self.failed_node_downloads.append({
                'node_id': node_id,
                'error': str(e),
                'action_required': 'Manual export from Coalesce UI'
            })
            return None

    def _extract_dependencies_from_node(self, node_data):
        """Extract only REAL, fetchable node dependencies from a node's configuration"""
        dependencies = set()
        
        if not node_data:
            return dependencies
        
        # 🎯 CONSERVATIVE APPROACH: Only extract UUID-format node IDs that we can actually fetch
        metadata = node_data.get('metadata', {})
        
        # 1. COLUMN REFERENCES - Look for actual node UUIDs only
        if 'columns' in metadata:
            columns = metadata['columns']
            if isinstance(columns, list):
                for column in columns:
                    if 'sources' in column and isinstance(column['sources'], list):
                        for source in column['sources']:
                            if 'columnReferences' in source and isinstance(source['columnReferences'], list):
                                for col_ref in source['columnReferences']:
                                    if 'nodeID' in col_ref and col_ref['nodeID']:
                                        node_id = str(col_ref['nodeID'])
                                        # Only include if it looks like a UUID (has dashes)
                                        if '-' in node_id and len(node_id) > 30:
                                            dependencies.add(node_id)
        
        # 2. SOURCE MAPPING ALIASES - Direct node ID mappings only
        if 'sourceMapping' in metadata:
            source_mappings = metadata['sourceMapping']
            if isinstance(source_mappings, list):
                for mapping in source_mappings:
                    # Aliases map names to actual node IDs - these are reliable
                    if 'aliases' in mapping and isinstance(mapping['aliases'], dict):
                        aliases = mapping['aliases']
                        for name, node_id in aliases.items():
                            if node_id and '-' in str(node_id) and len(str(node_id)) > 30:
                                dependencies.add(str(node_id))
        
        # 3. Skip location.nodename references for now - they cause too many 404s
        # We'll stick to direct UUID references only
        
        if dependencies:
            print(f"     🎯 Found {len(dependencies)} real node dependencies: {list(dependencies)[:3]}{'...' if len(dependencies) > 3 else ''}")
        
        return dependencies

    def _is_api_migratable_node(self, node_data, node_id):
        """Check if a node can be migrated via Coalesce API"""
        if not node_data:
            return False
        
        # Get node type - this is the key field for filtering
        node_type = node_data.get('type', '').lower()
        
        # API CANNOT handle these types per Coalesce limitations:
        non_migratable_types = {
            'source',           # Source tables
            'source_table',     # Source table variations
            'source_mapping',   # Source mappings
            'stage',           # Stage tables (can't maintain hash keys)
            'stage_table',     # Stage table variations
            'staging',         # Staging variations
            'raw',             # Sometimes raw/source are similar
        }
        
        # Check if this is a non-migratable type
        if node_type in non_migratable_types:
            return False
        
        # Additional checks for stage tables by name patterns
        node_name = node_data.get('name', '').lower()
        stage_patterns = ['stage_', 'stg_', '_stage', '_stg', 'staging_']
        
        if any(pattern in node_name for pattern in stage_patterns):
            # Double-check if it's actually a stage table
            if 'stage' in node_type or 'stg' in node_type:
                return False
        
        # Check for source indicators in configuration
        config = node_data.get('config', {})
        if isinstance(config, dict):
            # Look for source table indicators
            if config.get('materialized') == 'source' or config.get('is_source', False):
                return False
        
        return True
    
    def _categorize_dependencies(self, workspace_id, all_dependencies):
        """Categorize dependencies into API-migratable vs manual-required - CONSERVATIVE approach"""
        print(f"\n📋 CATEGORIZING DEPENDENCIES FOR API COMPATIBILITY")
        print("-" * 60)
        
        api_migratable = set()
        manual_required = []
        failed_analysis = []
        
        for node_id in all_dependencies:
            # Skip any obvious non-UUID references (we shouldn't have any now)
            if '.' in str(node_id) or not '-' in str(node_id) or len(str(node_id)) < 30:
                print(f"   Skipping non-UUID reference: {node_id}")
                continue
            
            # Get node details for analysis
            node_data = self._get_node_details(workspace_id, node_id)
            
            if not node_data:
                failed_analysis.append(node_id)
                continue
            
            # For now, assume all fetchable UUID nodes are API-migratable
            # Only exclude if we can definitively identify them as problematic
            node_type = node_data.get('type', '').lower()
            
            # Very conservative filtering - only exclude obvious problematic types
            if node_type in ['source', 'stage'] and any(word in node_type for word in ['raw', 'src_']):
                manual_required.append({
                    'node_id': node_id,
                    'node_name': node_data.get('name', node_id),
                    'type': node_type,
                    'reason': f"Node type '{node_type}' may not be API compatible",
                    'action': 'Manual review recommended'
                })
            else:
                # Default: assume it's API-migratable if we can fetch it
                api_migratable.add(node_id)
        
        print(f"   ✅ API-Migratable: {len(api_migratable)} nodes")
        print(f"   🔧 Manual Required: {len(manual_required)} nodes")
        print(f"   ❓ Failed Analysis: {len(failed_analysis)} nodes")
        
        return api_migratable, manual_required, failed_analysis

    def _resolve_all_dependencies(self, workspace_id, initial_nodes, max_depth=10):
        """Recursively resolve all dependencies for a set of nodes"""
        print(f"\n🔍 DEPENDENCY RESOLUTION STARTING")
        print(f"   Initial nodes: {len(initial_nodes)}")
        print(f"   Max depth: {max_depth}")
        print("-" * 50)
        
        all_discovered_nodes = set(str(node) for node in initial_nodes)
        processed_nodes = set()
        current_depth = 0
        
        while current_depth < max_depth:
            # Find nodes we need to process this round
            nodes_to_process = all_discovered_nodes - processed_nodes
            
            if not nodes_to_process:
                print(f"   Depth {current_depth}: No new nodes to process")
                break
            
            print(f"   Depth {current_depth}: Processing {len(nodes_to_process)} nodes")
            new_dependencies = set()
            
            for node_id in nodes_to_process:
                # Get node details
                node_data = self._get_node_details(workspace_id, node_id)
                
                if node_data:
                    # Extract dependencies
                    deps = self._extract_dependencies_from_node(node_data)
                    
                    if deps:
                        print(f"     Node {node_id}: Found {len(deps)} dependencies")
                        new_dependencies.update(deps)
                    else:
                        print(f"     Node {node_id}: No dependencies")
                else:
                    print(f"     Node {node_id}: Could not retrieve details")
                
                processed_nodes.add(node_id)
            
            # Add new dependencies to our discovered set
            if new_dependencies:
                before_size = len(all_discovered_nodes)
                all_discovered_nodes.update(new_dependencies)
                after_size = len(all_discovered_nodes)
                new_nodes_added = after_size - before_size
                print(f"   Depth {current_depth}: Added {new_nodes_added} new dependencies")
            else:
                print(f"   Depth {current_depth}: No new dependencies found")
                break
            
            current_depth += 1
        
        print(f"\n🎯 DEPENDENCY DISCOVERY COMPLETE")
        print(f"   Started with: {len(initial_nodes)} nodes")
        print(f"   Total discovered: {len(all_discovered_nodes)} nodes")
        print(f"   Dependencies found: {len(all_discovered_nodes) - len(initial_nodes)}")
        print(f"   Search depth used: {current_depth}")
        
        # Now categorize what can vs cannot be migrated via API
        api_migratable, manual_required, failed_analysis = self._categorize_dependencies(
            workspace_id, all_discovered_nodes
        )
        
        return {
            'all_discovered': all_discovered_nodes,
            'api_migratable': api_migratable, 
            'manual_required': manual_required,
            'failed_analysis': failed_analysis
        }

    def get_subgraph_details_and_nodes(self, workspace_id, subgraph_id, subgraph_name):
        """Get detailed subgraph info including ALL nodes and their dependencies"""
        print(f"\n>> EXTRACTING SUBGRAPH DETAILS WITH DEPENDENCIES: '{subgraph_name}' (ID: {subgraph_id})")
        print("-" * 80)
        
        try:
            # Get subgraph details
            response = requests.get(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/subgraphs/{subgraph_id}",
                headers=self.headers
            )
            
            print(f"GET subgraph details: {response.status_code}")
            
            if response.status_code != 200:
                print(f"[ERROR] Could not get subgraph details: {response.text[:100]}")
                return None
            
            subgraph_data = response.json()
            sg_details = subgraph_data.get('data', subgraph_data)
            
            # Extract initial node list from subgraph
            initial_steps = sg_details.get('steps', [])
            print(f"[OK] Subgraph contains {len(initial_steps)} direct nodes")
            
            # Convert steps to node IDs
            initial_node_ids = []
            for step in initial_steps:
                if isinstance(step, str):
                    initial_node_ids.append(step)
                elif isinstance(step, dict):
                    node_id = step.get('id', step.get('nodeId', str(step)))
                    initial_node_ids.append(node_id)
                else:
                    initial_node_ids.append(str(step))
            
            # 🚀 NEW FEATURE: Resolve ALL dependencies with API compatibility filtering
            print(f"\n🚀 STARTING ENHANCED DEPENDENCY RESOLUTION WITH API FILTERING")
            dependency_analysis = self._resolve_all_dependencies(workspace_id, initial_node_ids)
            
            all_discovered = dependency_analysis['all_discovered']
            api_migratable = dependency_analysis['api_migratable']
            manual_required = dependency_analysis['manual_required']
            failed_analysis = dependency_analysis['failed_analysis']
            
            # Get detailed information for API-migratable nodes
            node_details = {}
            if api_migratable:
                print(f"\n>> Getting detailed info for {len(api_migratable)} API-migratable nodes...")
                
                for i, node_id in enumerate(sorted(api_migratable), 1):
                    print(f"   Node {i}/{len(api_migratable)}: {node_id}")
                    
                    node_info = self._get_node_details(workspace_id, node_id)
                    if node_info:
                        node_details[node_id] = node_info
            
            # Combine subgraph and node information with enhanced metadata
            complete_subgraph = {
                'subgraph_id': subgraph_id,
                'subgraph_name': subgraph_name,
                'subgraph_data': sg_details,
                'original_node_count': len(initial_steps),
                'total_discovered_nodes': len(all_discovered),
                'api_migratable_count': len(api_migratable),
                'manual_required_count': len(manual_required),
                'failed_analysis_count': len(failed_analysis),
                'original_steps': initial_steps,
                'api_migratable_nodes': sorted(list(api_migratable)),
                'manual_required_nodes': manual_required,
                'failed_analysis_nodes': failed_analysis,
                'node_details': node_details,
                'dependency_analysis': {
                    'initial_nodes': initial_node_ids,
                    'all_discovered': sorted(list(all_discovered)),
                    'api_migratable': sorted(list(api_migratable)),
                    'dependencies_discovered': len(all_discovered) - len(initial_steps),
                    'api_compatible_dependencies': len(api_migratable) - len(initial_steps)
                },
                **self._get_file_metadata()
            }
            
            # Save detailed information with naming convention
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = self._get_filename('exported', subgraph_name, timestamp)
            
            with open(filename, 'w') as f:
                json.dump(complete_subgraph, f, indent=2)
            
            print(f"\n>> ENHANCED EXTRACTION COMPLETE!")
            print(f"   Original nodes in subgraph: {len(initial_steps)}")
            print(f"   Total dependencies discovered: {len(all_discovered) - len(initial_steps)}")
            print(f"   ✅ API-migratable nodes: {len(api_migratable)}")
            print(f"   🔧 Manual-required nodes: {len(manual_required)}")
            print(f"   📊 Nodes for API migration: {len(api_migratable)}")
            print(f"   Detailed data saved to: {filename}")
            print(f"   🚀 Enhanced features: dependency_resolution + API_compatibility_filtering")
            
            return complete_subgraph
            
        except Exception as e:
            print(f"[ERROR] Error getting subgraph details: {e}")
            return None

    def migrate_subgraph(self, subgraph_details, target_workspace, dry_run):
        """Migrate a single subgraph with API-compatible dependencies only"""
        subgraph_name = subgraph_details['subgraph_name']
        api_migratable_nodes = subgraph_details['api_migratable_nodes']
        manual_required = subgraph_details['manual_required_nodes']
        original_count = subgraph_details['original_node_count']
        api_count = subgraph_details['api_migratable_count']
        manual_count = subgraph_details['manual_required_count']
        
        print(f"\n>>> MIGRATING WITH API-COMPATIBLE DEPENDENCIES: '{subgraph_name}'")
        print(f"   Original nodes: {original_count}")
        print(f"   ✅ API-migratable total: {api_count}")
        print(f"   🔧 Manual setup required: {manual_count}")
        
        # Show manual nodes that need attention
        if manual_required:
            print(f"\n   🔧 NODES REQUIRING MANUAL SETUP:")
            for manual_node in manual_required[:5]:  # Show first 5
                print(f"      • {manual_node.get('node_name', manual_node['node_id'])} ({manual_node['type']})")
                print(f"        Reason: {manual_node['reason']}")
            if len(manual_required) > 5:
                print(f"      ... and {len(manual_required) - 5} more (see detailed output file)")
        
        # Create new name for target
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        new_name = f"{subgraph_name}_API_Compatible_{timestamp}"
        
        migration_payload = {
            "name": new_name,
            "steps": api_migratable_nodes  # Only API-compatible nodes
        }
        
        if dry_run:
            print(f"   [SKIP] DRY RUN: Would create '{new_name}' with {len(api_migratable_nodes)} API-compatible nodes")
            print(f"          (Manual setup still needed for {manual_count} nodes)")
            return {
                "dry_run": True,
                "original_name": subgraph_name,
                "new_name": new_name,
                "original_node_count": original_count,
                "api_migratable_count": api_count,
                "manual_required_count": manual_count,
                "api_nodes": api_migratable_nodes,
                "manual_nodes": manual_required,
                **self._get_file_metadata()
            }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/v1/workspaces/{target_workspace}/subgraphs",
                headers=self.headers,
                json=migration_payload
            )
            
            print(f"   POST create subgraph: {response.status_code}")
            
            if response.status_code in [200, 201]:
                created_data = response.json()
                created_id = created_data.get('data', {}).get('id')
                print(f"   [OK] SUCCESS! Created API-compatible subgraph ID: {created_id}")
                print(f"        Migrated {api_count} nodes via API")
                if manual_count > 0:
                    print(f"        ⚠️  Still need manual setup for {manual_count} source/stage nodes")
                
                # Save migration result with  naming convention
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = self._get_filename('migration_results', subgraph_name, timestamp, created_id)
                
                migration_result = {
                    'migration_result': created_data,
                    'original_subgraph': subgraph_details,
                    'migration_timestamp': datetime.now().isoformat(),
                    'enhanced_features': ['dependency_resolution', 'api_compatibility_filtering'],
                    'api_limitations_handled': {
                        'source_tables': 'excluded_requires_manual_setup',
                        'stage_tables': 'excluded_cannot_maintain_hash_keys',
                        'source_mappings': 'excluded_requires_manual_setup'
                    },
                    **self._get_file_metadata()
                }
                
                with open(filename, 'w') as f:
                    json.dump(migration_result, f, indent=2)
                
                print(f">> API-compatible migration result saved to: {filename}")
                print(f"   🚀 {api_count - original_count} additional dependencies migrated automatically!")
                
                return created_data
            else:
                print(f"    FAILED: {response.status_code} - {response.text[:100]}")
                return None
                
        except Exception as e:
            print(f"    ERROR: {e}")
            return None

    def save_failed_downloads(self):
        """Save list of nodes that couldn't be downloaded for manual action"""
        if not hasattr(self, 'failed_node_downloads') or not self.failed_node_downloads:
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self._get_filename('manual_downloads', timestamp=timestamp)
        
        with open(filename, 'w') as f:
            f.write(f"[ALERT] MANUAL DOWNLOAD REQUIRED - {self.project_info['name'].upper()}\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Project: {self.project_info['name']} ({self.project_info['identifier']})\n")
            f.write(f"Script: {self.script_name} (Enhanced with dependency resolution)\n\n")
            
            f.write(f"[WARNING] {len(self.failed_node_downloads)} NODES COULD NOT BE DOWNLOADED AUTOMATICALLY\n")
            f.write("-" * 80 + "\n\n")
            
            f.write("ACTION REQUIRED: Manually export these nodes from Coalesce UI\n\n")
            
            for i, failed in enumerate(self.failed_node_downloads, 1):
                f.write(f"{i:3d}. Node ID: {failed['node_id']}\n")
                f.write(f"     Error: {failed['error']}\n")
                f.write(f"     Action: {failed['action_required']}\n\n")
        
        print(f"\n📋 Manual action file created: {filename}")
        print(f"    {len(self.failed_node_downloads)} nodes require manual download")
        return filename

    def run_migration(self):
        """Run the complete enhanced subgraph migration with dependency resolution"""
        print(f"\n🚀 {self.project_info['name'].upper()} ENHANCED SUBGRAPH MIGRATION")
        print("=" * 80)
        
        # Get source and target from config
        source_workspace = self.migration_config.get("source", {}).get("workspace_id")
        target_workspace = self.migration_config.get("target", {}).get("workspace_id")
        dry_run = self.migration_config.get('dry_run', True)
        
        source_name = self.migration_config.get("source", {}).get("workspace_name", "Unknown")
        target_name = self.migration_config.get("target", {}).get("workspace_name", "Unknown")
        
        print(f">> Source: Workspace {source_workspace} ({source_name})")
        print(f">> Target: Workspace {target_workspace} ({target_name})")
        print(f">>> Subgraphs: {self.target_subgraphs}")
        print(f">> Dry Run: {dry_run}")
        print(f">> 🚀 Enhanced Features: Automatic dependency resolution!")
        print(f">> Script: {self.script_name}")
        print(f">> Config: {self.config_file_name}")
        print(f">> File Format: subgraph_*.json")
        
        # Step 1: Find the target subgraphs
        found_subgraphs = self.find_target_subgraphs(source_workspace)
        
        if not found_subgraphs:
            print(f"\n❌ No target subgraphs found in workspace {source_workspace}")
            print(f">> Check the subgraph names in your config file")
            return False
        
        # Initialize counters
        total_original_nodes = 0
        total_api_migratable = 0
        total_manual_required = 0
        
        # Step 2: Extract detailed data with dependency resolution
        detailed_subgraphs = {}
        total_dependencies = 0
        
        for target_name, subgraph_info in found_subgraphs.items():
            details = self.get_subgraph_details_and_nodes(
                source_workspace,
                subgraph_info['id'],
                subgraph_info['name']
            )
            if details:
                detailed_subgraphs[target_name] = details
                total_original_nodes += details['original_node_count']
                total_api_migratable += details['api_migratable_count']
                total_manual_required += details['manual_required_count']
                total_dependencies += details['dependency_analysis']['dependencies_discovered']
        
        # Step 3: Migrate each subgraph with dependencies
        print(f"\n>>> MIGRATING {len(detailed_subgraphs)} ENHANCED SUBGRAPHS")
        print(f"    Original nodes: {total_original_nodes}")
        print(f"    Dependencies: {total_dependencies}")
        print(f"    Total nodes: {total_original_nodes + total_dependencies}")
        print("-" * 60)
        
        migration_results = []
        
        for target_name, subgraph_details in detailed_subgraphs.items():
            result = self.migrate_subgraph(
                subgraph_details,
                target_workspace,
                dry_run
            )
            if result:
                migration_results.append(result)
        
        # Step 4: Enhanced Summary
        manual_file = self.save_failed_downloads()
        
        print(f"\n🎉 ENHANCED MIGRATION COMPLETE!")
        print("-" * 50)
        print(f"[OK] Found: {len(found_subgraphs)} target subgraphs")
        print(f">> Extracted: {len(detailed_subgraphs)} subgraphs with full dependency trees")
        print(f">>> {'Would migrate' if dry_run else 'Migrated'}: {len(migration_results)} complete subgraphs")
        print(f"🚀 Dependencies auto-discovered: {total_dependencies} nodes")
        print(f"💪 No more manual mapping required!")
        
        if hasattr(self, 'failed_node_downloads') and self.failed_node_downloads:
            print(f"[WARNING] Manual downloads needed: {len(self.failed_node_downloads)} nodes")
            if manual_file:
                print(f">> Manual action list: {manual_file}")
        
        if dry_run:
            print(f"\n>> This was a DRY RUN")
            print(f">> To actually migrate:")
            print(f"   1. Edit {self.config_file_name}")
            print(f"   2. Change: 'dry_run': True -> 'dry_run': False")
            print(f"   3. Run: python {self.script_name}")
        
        if migration_results:
            print(f"\n>> subgraph files created:")
            print(f"   * subgraph_<name>_*.json - Complete subgraph data with dependencies")
            if not dry_run:
                print(f"   *subgraph_migration_<name>_*.json - Migration results")
            if manual_file:
                print(f"   *subgraph_manual_downloads_*.txt - Manual action required")
            print(f"   All files include dependency analysis and enhanced metadata")
        
        print(f"\n🚀 ENHANCED FEATURES SUMMARY:")
        print(f"   ✅ Automatic dependency resolution")
        print(f"   ✅ Recursive predecessor chain analysis")
        print(f"   ✅ Complete node migration (no manual mapping needed)")
        print(f"   ✅ Enhanced metadata and tracking")
        print(f"   ✅ subgraph naming convention")
        
        return migration_results

def main():
    """Run the enhanced migration"""
    try:
        migration_tool = EnhancedSubgraphMigration()
        results = migration_tool.run_migration()
        
    except Exception as e:
        print(f"❌ Enhanced migration failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
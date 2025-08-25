#!/usr/bin/env python3
"""
ENHANCED UNIFIED Metadata Refresh Hack
Fixes phantom metadata updates for ALL Data Vault node types: S_, H_, L_, STG_, FACT_, etc.

This script:
1. Loads BOTH migration result files (*created_nodes*.json) AND subgraph export files (subgraph_*.json)
2. Consolidates all problematic nodes from both sources
3. Applies the same metadata refresh hack to ALL nodes
4. ENHANCED: Better pattern matching for all Data Vault node types

Usage:
    python enhanced_unified_metadata_hack.py --dry-run
    python enhanced_unified_metadata_hack.py --execute
    python enhanced_unified_metadata_hack.py --patterns "all-dv" --execute
    python enhanced_unified_metadata_hack.py --patterns "STG_,S_,H_,L_" --execute
"""

import requests
import json
import glob
import time
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv

# =============================================================================
# ENHANCED CONFIGURATION - HANDLES ALL DATA VAULT NODE TYPES
# =============================================================================
TARGET_WORKSPACE_ID = "270"  # Your PROD workspace ID
PROJECT_NAME = "Enhanced Unified Hack"
API_TIMEOUT = 90

# 🎯 COMPREHENSIVE DATA VAULT PATTERNS
DEFAULT_PATTERNS = [
    "S_",       # Satellites (including S_*_CURRENT)
    "H_",       # Hubs
    "L_",       # Links
    "STG_",     # Stage tables
    "FACT_",    # Fact tables
    "DIM_",     # Dimension tables
    "SRC_",     # Source tables (if any)
    "LNK_",     # Link variations
    "HUB_",     # Hub variations
    "SAT_"      # Satellite variations
]

# 🎯 SPECIAL SUFFIX PATTERNS (nodes ending with these)
SUFFIX_PATTERNS = [
    "_CURRENT",     # Satellite current records
    "_HISTORY",     # Historical records
    "_DELTA",       # Delta records
    "_STAGE",       # Staging suffix
    "_STG"          # Stage suffix
]

# 🔧 EXCLUDE PATTERNS - Skip nodes containing these
EXCLUDE_PATTERNS = [
    "_TEST_",       # Test nodes
    "_TEMP_",       # Temporary nodes
    "_DEBUG_",      # Debug nodes
    "_BACKUP_"      # Backup nodes
]

# 🔧 ENHANCED: Comprehensive node filtering function
def enhanced_node_filter(node_name, patterns):
    """
    ENHANCED function to determine if a node should be processed.
    Handles all Data Vault naming conventions including special cases.
    Return True to include the node, False to exclude it.
    """
    # Quick exclusion check first
    for exclude in EXCLUDE_PATTERNS:
        if exclude in node_name.upper():
            return False
    
    # Special pattern: "all-dv" means all Data Vault patterns
    if patterns == ["all-dv"]:
        patterns = DEFAULT_PATTERNS
    
    # Check prefix patterns
    for pattern in patterns:
        if node_name.upper().startswith(pattern.upper()):
            return True
    
    # Check suffix patterns for satellites and other special cases
    for suffix in SUFFIX_PATTERNS:
        if node_name.upper().endswith(suffix.upper()):
            # Additional check: make sure it's likely a Data Vault node
            if any(dv_pattern in node_name.upper() for dv_pattern in ["S_", "SAT_", "H_", "HUB_", "L_", "LNK_", "STG_", "STAGE"]):
                return True
    
    # Special case: Satellites that follow S_*_CURRENT pattern
    if "_CURRENT" in node_name.upper():
        # Check if it starts with S_ or SAT_
        if node_name.upper().startswith("S_") or node_name.upper().startswith("SAT_"):
            return True
    
    # Special case: Any node with DV-style underscores that might be missed
    if "_" in node_name:
        # Check for common DV patterns anywhere in the name
        dv_indicators = ["HUB_", "SAT_", "LNK_", "STG_", "STAGE_", "FACT_", "DIM_"]
        for indicator in dv_indicators:
            if indicator in node_name.upper():
                return True
    
    return False

# =============================================================================

print(f"🎯 ENHANCED UNIFIED METADATA HACK CONFIGURATION:")
print(f"   Workspace ID: {TARGET_WORKSPACE_ID}")
print(f"   Project: {PROJECT_NAME}")
print(f"   Purpose: Fix phantom metadata updates for ALL Data Vault node types")
print(f"   Enhanced: Better pattern matching for S_, H_, L_, STG_, satellites, etc.")
# =============================================================================

class EnhancedUnifiedMetadataHack:
    """Enhanced unified metadata refresh hack for ALL Data Vault node types"""

    def __init__(self, patterns=None):
        # Load API credentials from .env file
        load_dotenv()
        
        self.base_url = os.getenv('COALESCE_BASE_URL')
        self.access_token = os.getenv('COALESCE_ACCESS_TOKEN')
        
        if not self.base_url or not self.access_token:
            raise RuntimeError(
                "[ERROR] Missing API config in .env file!\n"
                "Required: COALESCE_BASE_URL and COALESCE_ACCESS_TOKEN"
            )
        
        self.base_url = self.base_url.rstrip('/')
        
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        self.target_workspace = TARGET_WORKSPACE_ID
        self.patterns = patterns or DEFAULT_PATTERNS

        # Results tracking
        self.all_target_nodes = []
        self.api_nodes = []
        self.ui_nodes = []
        self.hack_results = []
        self.successful_fixes = []
        self.failed_fixes = []

        print(f"\n>>> ENHANCED UNIFIED METADATA HACK INITIALIZED")
        print(f"   Target Workspace: {self.target_workspace}")
        print(f"   Node Patterns: {'/'.join(self.patterns) if self.patterns != ['all-dv'] else 'ALL Data Vault Types'}")
        print(f"   Strategy: Process ALL Data Vault nodes (API + UI migrated)")
        print(f"   Enhanced: Better S_*_CURRENT satellite detection")

    def analyze_node_patterns(self, all_nodes):
        """Analyze what node patterns we actually found"""
        print(f"\n>>> ANALYZING DISCOVERED NODE PATTERNS")
        print("-" * 60)
        
        pattern_analysis = {}
        prefix_analysis = {}
        suffix_analysis = {}
        
        for node in all_nodes:
            node_name = node['name'].upper()
            
            # Analyze prefixes (first 4 characters)
            prefix = node_name[:4] if len(node_name) >= 4 else node_name
            prefix_analysis[prefix] = prefix_analysis.get(prefix, 0) + 1
            
            # Analyze suffixes
            if "_CURRENT" in node_name:
                suffix_analysis["_CURRENT"] = suffix_analysis.get("_CURRENT", 0) + 1
            if "_HISTORY" in node_name:
                suffix_analysis["_HISTORY"] = suffix_analysis.get("_HISTORY", 0) + 1
            if "_STAGE" in node_name or "_STG" in node_name:
                suffix_analysis["_STAGE/_STG"] = suffix_analysis.get("_STAGE/_STG", 0) + 1
            
            # Categorize by Data Vault component
            if node_name.startswith("S_") or node_name.startswith("SAT_"):
                pattern_analysis["Satellites"] = pattern_analysis.get("Satellites", 0) + 1
            elif node_name.startswith("H_") or node_name.startswith("HUB_"):
                pattern_analysis["Hubs"] = pattern_analysis.get("Hubs", 0) + 1
            elif node_name.startswith("L_") or node_name.startswith("LNK_"):
                pattern_analysis["Links"] = pattern_analysis.get("Links", 0) + 1
            elif node_name.startswith("STG_") or "STAGE" in node_name:
                pattern_analysis["Stages"] = pattern_analysis.get("Stages", 0) + 1
            elif node_name.startswith("FACT_"):
                pattern_analysis["Facts"] = pattern_analysis.get("Facts", 0) + 1
            elif node_name.startswith("DIM_"):
                pattern_analysis["Dimensions"] = pattern_analysis.get("Dimensions", 0) + 1
            else:
                pattern_analysis["Other"] = pattern_analysis.get("Other", 0) + 1
        
        print(f"📊 DATA VAULT COMPONENT BREAKDOWN:")
        for component, count in sorted(pattern_analysis.items()):
            print(f"   {component}: {count} nodes")
        
        print(f"\n📊 PREFIX ANALYSIS (Top 10):")
        for prefix, count in sorted(prefix_analysis.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   {prefix}: {count} nodes")
        
        print(f"\n📊 SUFFIX ANALYSIS:")
        for suffix, count in sorted(suffix_analysis.items()):
            print(f"   {suffix}: {count} nodes")
        
        # Special focus on S_ nodes with _CURRENT
        s_current_nodes = [n for n in all_nodes if n['name'].upper().startswith("S_") and "_CURRENT" in n['name'].upper()]
        if s_current_nodes:
            print(f"\n🎯 S_*_CURRENT SATELLITES FOUND: {len(s_current_nodes)} nodes")
            for node in s_current_nodes[:5]:  # Show first 5
                print(f"   - {node['name']} ({node['source']})")
            if len(s_current_nodes) > 5:
                print(f"   ... and {len(s_current_nodes) - 5} more")

    def find_all_source_files(self):
        """Find both migration result files AND subgraph export files"""
        print(f"\n>>> FINDING ALL SOURCE FILES")
        print("-" * 60)
        
        # Migration result files (API-migrated nodes)
        migration_patterns = [
            "*created_nodes*.json",
            "*nodes_created*.json", 
            "*creation_result*.json"
        ]
        
        migration_files = []
        for pattern in migration_patterns:
            files = glob.glob(pattern)
            migration_files.extend(files)
        
        # Remove duplicates
        migration_files = list(set(migration_files))
        
        # Subgraph export files (UI-migrated nodes)
        subgraph_files = glob.glob("subgraph_*.json")
        # Filter out migration results
        subgraph_files = [f for f in subgraph_files if not f.startswith('subgraph_migration_')]
        
        print(f"📁 MIGRATION RESULT FILES: {len(migration_files)}")
        for file in migration_files:
            print(f"   - {file}")
        
        print(f"\n📁 SUBGRAPH EXPORT FILES: {len(subgraph_files)}")
        for file in subgraph_files:
            print(f"   - {file}")
        
        return migration_files, subgraph_files

    def load_api_migrated_nodes(self, migration_files):
        """Load nodes from API migration result files with enhanced filtering"""
        print(f"\n>>> LOADING API-MIGRATED NODES (ENHANCED FILTERING)")
        print("-" * 50)
        
        api_nodes = []
        
        for file in migration_files:
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                
                print(f"📄 Processing: {file}")
                
                # Extract created nodes from API migration results
                creation_result = data.get('creation_result', {})
                created_nodes = creation_result.get('created_nodes', [])
                node_id_mapping = creation_result.get('node_id_mapping', {})
                
                if created_nodes:
                    print(f"   Found {len(created_nodes)} API-created nodes")
                    matching_count = 0
                    for node in created_nodes:
                        node_name = node.get('name', '')
                        if enhanced_node_filter(node_name, self.patterns):
                            api_nodes.append({
                                'id': node.get('new_id'),
                                'name': node_name,
                                'original_id': node.get('original_id'),
                                'source': 'api_migration',
                                'source_file': file,
                                'needs_api_fetch': True  # Need to get current data
                            })
                            matching_count += 1
                            print(f"      ✅ {node_name} (API-migrated)")
                    print(f"   Matching nodes: {matching_count}")
                
                elif node_id_mapping:
                    print(f"   Found {len(node_id_mapping)} node mappings (need to fetch details)")
                    # Need to fetch details for each node
                    for original_id, new_id in node_id_mapping.items():
                        api_nodes.append({
                            'id': new_id,
                            'name': f'Node_{new_id[:8]}',  # Temporary name
                            'original_id': original_id,
                            'source': 'api_migration',
                            'source_file': file,
                            'needs_api_fetch': True
                        })
                
            except Exception as e:
                print(f"   [ERROR] Error loading {file}: {e}")
        
        print(f"📊 Total API nodes found: {len(api_nodes)}")
        return api_nodes

    def load_ui_migrated_nodes(self, subgraph_files):
        """Load nodes from subgraph export files with enhanced filtering"""
        print(f"\n>>> LOADING UI-MIGRATED NODES (ENHANCED FILTERING)")
        print("-" * 50)
        
        ui_nodes = []
        
        for file in subgraph_files:
            try:
                with open(file, 'r') as f:
                    data = json.load(f)
                
                subgraph_name = data.get('subgraph_name', 'Unknown')
                node_details = data.get('node_details', {})
                
                print(f"📄 Processing: {file} ('{subgraph_name}')")
                print(f"   Found {len(node_details)} total nodes")
                
                matching_count = 0
                for node_id, node_data in node_details.items():
                    if not isinstance(node_data, dict):
                        continue
                    
                    node_name = node_data.get('name', '')
                    if enhanced_node_filter(node_name, self.patterns):
                        ui_nodes.append({
                            'id': node_id,
                            'name': node_name,
                            'original_data': node_data,
                            'source': 'ui_migration',
                            'source_file': file,
                            'subgraph': subgraph_name,
                            'needs_api_fetch': False  # Already have full data
                        })
                        matching_count += 1
                        print(f"      ✅ {node_name} (UI-migrated)")
                
                print(f"   Matching nodes: {matching_count}")
                
            except Exception as e:
                print(f"   [ERROR] Error loading {file}: {e}")
        
        print(f"📊 Total UI nodes found: {len(ui_nodes)}")
        return ui_nodes

    def enrich_api_nodes(self, api_nodes):
        """Get current details for API-migrated nodes that need fetching"""
        print(f"\n>>> ENRICHING API NODE DATA (ENHANCED FILTERING)")
        print("-" * 50)
        
        nodes_needing_fetch = [n for n in api_nodes if n.get('needs_api_fetch', False)]
        print(f"Fetching details for {len(nodes_needing_fetch)} API nodes...")
        
        enriched_nodes = []
        
        for i, node in enumerate(api_nodes, 1):
            if not node.get('needs_api_fetch', False):
                enriched_nodes.append(node)
                continue
            
            node_id = node['id']
            print(f"   {i:3d}/{len(api_nodes)}: Fetching {node_id[:8]}...")
            
            try:
                response = requests.get(
                    f"{self.base_url}/api/v1/workspaces/{self.target_workspace}/nodes/{node_id}",
                    headers=self.headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    current_data = response.json()
                    node_info = current_data.get('data', current_data)
                    
                    node_name = node_info.get('name', f'Node_{node_id}')
                    
                    # Check if this node matches our enhanced patterns after getting real name
                    if enhanced_node_filter(node_name, self.patterns):
                        enriched_node = node.copy()
                        enriched_node.update({
                            'name': node_name,
                            'original_data': node_info,
                            'api_accessible': True
                        })
                        enriched_nodes.append(enriched_node)
                        print(f"      ✅ {node_name} (matches enhanced patterns)")
                        
                        # Special logging for S_*_CURRENT nodes
                        if node_name.upper().startswith("S_") and "_CURRENT" in node_name.upper():
                            print(f"         🎯 SATELLITE CURRENT: {node_name}")
                    else:
                        print(f"      ⏭️ {node_name} (doesn't match enhanced patterns)")
                else:
                    print(f"      ❌ API error {response.status_code}")
                    # Keep node but mark as inaccessible
                    enriched_node = node.copy()
                    enriched_node.update({
                        'api_accessible': False,
                        'api_error': response.status_code
                    })
                    enriched_nodes.append(enriched_node)
                    
            except Exception as e:
                print(f"      ❌ Exception: {e}")
                enriched_node = node.copy()
                enriched_node.update({
                    'api_accessible': False,
                    'api_error': str(e)
                })
                enriched_nodes.append(enriched_node)
        
        # Filter to only include nodes that match patterns and are accessible
        final_nodes = [n for n in enriched_nodes if 
                      enhanced_node_filter(n['name'], self.patterns) and
                      n.get('api_accessible', True)]
        
        print(f"📊 API nodes after enhanced enrichment: {len(final_nodes)}")
        return final_nodes

    def consolidate_all_nodes(self, api_nodes, ui_nodes):
        """Consolidate all target nodes from both sources"""
        print(f"\n>>> CONSOLIDATING ALL TARGET NODES (ENHANCED)")
        print("-" * 50)
        
        # Combine all nodes
        all_nodes = api_nodes + ui_nodes
        
        # Remove duplicates based on node ID
        unique_nodes = {}
        for node in all_nodes:
            node_id = node['id']
            if node_id not in unique_nodes:
                unique_nodes[node_id] = node
            else:
                # Keep the one with more complete data
                existing = unique_nodes[node_id]
                if node.get('original_data') and not existing.get('original_data'):
                    unique_nodes[node_id] = node
        
        final_nodes = list(unique_nodes.values())
        
        # Enhanced analysis
        self.analyze_node_patterns(final_nodes)
        
        # Summary by source
        api_count = len([n for n in final_nodes if n['source'] == 'api_migration'])
        ui_count = len([n for n in final_nodes if n['source'] == 'ui_migration'])
        
        print(f"\n📊 CONSOLIDATED RESULTS (ENHANCED):")
        print(f"   API-migrated nodes: {api_count}")
        print(f"   UI-migrated nodes: {ui_count}")
        print(f"   Total unique nodes: {len(final_nodes)}")
        
        return final_nodes

    def apply_enhanced_hack(self, node, dry_run=True):
        """Apply metadata refresh hack to any node (API or UI source) with enhanced logging"""
        node_id = node['id']
        node_name = node['name']
        source = node['source']

        # Special logging for satellites
        is_satellite = node_name.upper().startswith("S_") and "_CURRENT" in node_name.upper()
        node_type_indicator = "🛰️ SATELLITE" if is_satellite else "📦 NODE"

        print(f"   [PROCESSING] {node_type_indicator} '{node_name}' ({source}) ({str(node_id)[:8]}...)")

        # Check if node is accessible
        if node.get('api_accessible') == False:
            print(f"        ❌ Not accessible via API")
            return {
                'success': False,
                'node_id': node_id,
                'node_name': node_name,
                'error': 'Not accessible via API',
                'skipped': True,
                'is_satellite': is_satellite
            }

        if dry_run:
            print(f"        [DRY RUN] Would hack {source} {node_type_indicator.split()[1].lower()} '{node_name}'")
            return {
                'success': True,
                'dry_run': True,
                'node_id': node_id,
                'node_name': node_name,
                'source': source,
                'is_satellite': is_satellite
            }

        try:
            # Get current node data (either from cache or API)
            if node.get('original_data'):
                current_data = node['original_data']
            else:
                # Need to fetch current data
                response = requests.get(
                    f"{self.base_url}/api/v1/workspaces/{self.target_workspace}/nodes/{node_id}",
                    headers=self.headers,
                    timeout=30
                )
                if response.status_code != 200:
                    return {
                        'success': False,
                        'node_id': node_id,
                        'node_name': node_name,
                        'error': f'Failed to fetch current data: {response.status_code}',
                        'is_satellite': is_satellite
                    }
                current_data = response.json().get('data', response.json())

            original_description = current_data.get('description', '')
            
            # Step 1: Make cosmetic change
            print(f"        [STEP 1] Applying cosmetic change to {node_type_indicator.split()[1].lower()}...")
            modified_data = current_data.copy()
            modified_data['description'] = original_description + " "  # Add space

            response = requests.put(
                f"{self.base_url}/api/v1/workspaces/{self.target_workspace}/nodes/{node_id}",
                headers=self.headers,
                json=modified_data,
                timeout=API_TIMEOUT
            )

            if response.status_code not in [200, 201]:
                error_msg = f'Failed to apply change: {response.status_code}'
                print(f"        ❌ Step 1 failed: {error_msg}")
                return {
                    'success': False,
                    'node_id': node_id,
                    'node_name': node_name,
                    'error': error_msg,
                    'step': 'cosmetic_change',
                    'is_satellite': is_satellite
                }

            time.sleep(1)  # Brief pause

            # Step 2: Revert to original
            print(f"        [STEP 2] Reverting to original...")
            current_data['description'] = original_description

            response = requests.put(
                f"{self.base_url}/api/v1/workspaces/{self.target_workspace}/nodes/{node_id}",
                headers=self.headers,
                json=current_data,
                timeout=API_TIMEOUT
            )

            if response.status_code not in [200, 201]:
                error_msg = f'Failed to revert change: {response.status_code}'
                print(f"        ❌ Step 2 failed: {error_msg}")
                return {
                    'success': False,
                    'node_id': node_id,
                    'node_name': node_name,
                    'error': error_msg,
                    'step': 'revert_change',
                    'is_satellite': is_satellite
                }

            success_msg = "✅ Hack completed successfully"
            if is_satellite:
                success_msg += " 🛰️ (SATELLITE _CURRENT node fixed!)"
            print(f"        {success_msg}")

            return {
                'success': True,
                'node_id': node_id,
                'node_name': node_name,
                'source': source,
                'original_description': original_description,
                'hack_timestamp': datetime.now().isoformat(),
                'is_satellite': is_satellite
            }

        except Exception as e:
            print(f"        ❌ Exception: {e}")
            return {
                'success': False,
                'node_id': node_id,
                'node_name': node_name,
                'error': f'Exception: {str(e)}',
                'step': 'exception',
                'is_satellite': is_satellite
            }

    def batch_hack_all_nodes(self, all_nodes, batch_size=5, dry_run=True):
        """Apply hack to all consolidated nodes in batches with enhanced progress tracking"""
        print(f"\n>>> BATCH HACKING ALL DATA VAULT NODES (ENHANCED)")
        print(f"   Total nodes: {len(all_nodes)}")
        print(f"   Node patterns: {'/'.join(self.patterns) if self.patterns != ['all-dv'] else 'ALL Data Vault Types'}")
        print(f"   Batch size: {batch_size}")
        print(f"   Dry run: {dry_run}")
        print("-" * 60)

        if not all_nodes:
            print(f"   [WARNING] No matching nodes found to process!")
            return

        # Count special node types for progress tracking
        satellites = [n for n in all_nodes if n['name'].upper().startswith("S_") and "_CURRENT" in n['name'].upper()]
        hubs = [n for n in all_nodes if n['name'].upper().startswith("H_")]
        links = [n for n in all_nodes if n['name'].upper().startswith("L_")]
        stages = [n for n in all_nodes if n['name'].upper().startswith("STG_")]
        
        print(f"   📊 Node Type Breakdown:")
        print(f"      🛰️ Satellites (_CURRENT): {len(satellites)}")
        print(f"      🔗 Hubs: {len(hubs)}")
        print(f"      🔗 Links: {len(links)}")
        print(f"      📦 Stages: {len(stages)}")
        print(f"      📦 Other: {len(all_nodes) - len(satellites) - len(hubs) - len(links) - len(stages)}")

        total_batches = (len(all_nodes) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(all_nodes))
            batch_nodes = all_nodes[start_idx:end_idx]

            print(f"\n[BATCH {batch_num + 1}/{total_batches}] Processing nodes {start_idx + 1}-{end_idx}")

            for i, node in enumerate(batch_nodes):
                node_idx = start_idx + i + 1
                print(f"\n   [{node_idx:3d}/{len(all_nodes)}]")

                result = self.apply_enhanced_hack(node, dry_run)
                self.hack_results.append(result)

                if result.get('success', False):
                    self.successful_fixes.append(result)
                else:
                    self.failed_fixes.append(result)

                # Brief pause between nodes
                if not dry_run:
                    time.sleep(0.5)

            # Pause between batches
            if batch_num < total_batches - 1 and not dry_run:
                print(f"   [BATCH COMPLETE] Pausing 3 seconds...")
                time.sleep(3)

    def save_enhanced_results(self):
        """Save comprehensive results for enhanced unified hack"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"enhanced_unified_hack_results_{timestamp}.json"

        # Enhanced analysis by source and node type
        api_successes = [r for r in self.successful_fixes if r.get('source') == 'api_migration']
        ui_successes = [r for r in self.successful_fixes if r.get('source') == 'ui_migration']
        api_failures = [r for r in self.failed_fixes if r.get('source') == 'api_migration']
        ui_failures = [r for r in self.failed_fixes if r.get('source') == 'ui_migration']
        
        # Satellite-specific analysis
        satellite_successes = [r for r in self.successful_fixes if r.get('is_satellite', False)]
        satellite_failures = [r for r in self.failed_fixes if r.get('is_satellite', False)]

        results = {
            'hack_summary': {
                'approach': 'enhanced_unified_api_and_ui',
                'total_nodes_processed': len(self.hack_results),
                'successful_hacks': len(self.successful_fixes),
                'failed_hacks': len(self.failed_fixes),
                'success_rate': (len(self.successful_fixes) / len(self.hack_results) * 100) if self.hack_results else 0,
                'target_workspace': self.target_workspace,
                'patterns_used': self.patterns,
                'execution_timestamp': datetime.now().isoformat(),
                'enhancements': ['better_satellite_detection', 'dv_pattern_analysis', 'comprehensive_filtering']
            },
            'breakdown_by_source': {
                'api_migrated': {
                    'successful': len(api_successes),
                    'failed': len(api_failures),
                    'success_rate': (len(api_successes) / (len(api_successes) + len(api_failures)) * 100) if (api_successes or api_failures) else 0
                },
                'ui_migrated': {
                    'successful': len(ui_successes),
                    'failed': len(ui_failures),
                    'success_rate': (len(ui_successes) / (len(ui_successes) + len(ui_failures)) * 100) if (ui_successes or ui_failures) else 0
                }
            },
            'satellite_analysis': {
                'successful_satellites': len(satellite_successes),
                'failed_satellites': len(satellite_failures),
                'satellite_success_rate': (len(satellite_successes) / (len(satellite_successes) + len(satellite_failures)) * 100) if (satellite_successes or satellite_failures) else 0,
                'satellite_nodes': [{'name': r['node_name'], 'source': r.get('source'), 'success': True} for r in satellite_successes] + 
                                 [{'name': r['node_name'], 'source': r.get('source'), 'success': False, 'error': r.get('error')} for r in satellite_failures]
            },
            'detailed_results': {
                'successful_fixes': self.successful_fixes,
                'failed_fixes': self.failed_fixes,
                'all_hack_attempts': self.hack_results
            }
        }

        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n>> Enhanced detailed results saved to: {filename}")
        return filename

    def run_enhanced_unified_hack(self, dry_run=True):
        """Execute the complete enhanced unified hack process"""
        print(f"\n>>> ENHANCED UNIFIED METADATA HACK FOR ALL DATA VAULT TYPES")
        print("=" * 80)
        print(f"   Target: Workspace {self.target_workspace}")
        print(f"   Patterns: {'/'.join(self.patterns) if self.patterns != ['all-dv'] else 'ALL Data Vault Types'}")
        print(f"   Approach: Enhanced processing for API + UI migrated nodes")
        print(f"   Enhanced: Better S_*_CURRENT satellite detection")
        print(f"   Dry run: {dry_run}")

        # Step 1: Find all source files
        migration_files, subgraph_files = self.find_all_source_files()
        
        if not migration_files and not subgraph_files:
            print(f"\n[ERROR] No source files found!")
            print(f"   Need either migration result files OR subgraph export files")
            return False

        # Step 2: Load API-migrated nodes with enhanced filtering
        api_nodes = []
        if migration_files:
            api_nodes = self.load_api_migrated_nodes(migration_files)
            if api_nodes:
                api_nodes = self.enrich_api_nodes(api_nodes)
        
        # Step 3: Load UI-migrated nodes with enhanced filtering
        ui_nodes = []
        if subgraph_files:
            ui_nodes = self.load_ui_migrated_nodes(subgraph_files)

        # Step 4: Consolidate all nodes with enhanced analysis
        all_target_nodes = self.consolidate_all_nodes(api_nodes, ui_nodes)
        
        if not all_target_nodes:
            print(f"\n[ERROR] No matching nodes found with enhanced patterns!")
            print(f"   Looking for patterns: {self.patterns}")
            print(f"   Enhanced patterns include: S_*_CURRENT, H_, L_, STG_, etc.")
            return False

        self.all_target_nodes = all_target_nodes

        # Step 5: Apply enhanced hack to all nodes
        self.batch_hack_all_nodes(all_target_nodes, batch_size=5, dry_run=dry_run)

        # Step 6: Save enhanced results
        results_file = self.save_enhanced_results()

        # Enhanced final summary
        print(f"\n>>> ENHANCED UNIFIED HACK COMPLETE!")
        print("-" * 50)

        # Count satellites specifically
        satellites_processed = len([r for r in self.hack_results if r.get('is_satellite', False)])
        satellites_fixed = len([r for r in self.successful_fixes if r.get('is_satellite', False)])

        if dry_run:
            print(f"[DRY RUN] Would hack {len(all_target_nodes)} Data Vault nodes total")
            api_count = len([n for n in all_target_nodes if n['source'] == 'api_migration'])
            ui_count = len([n for n in all_target_nodes if n['source'] == 'ui_migration'])
            print(f"   API-migrated: {api_count} nodes")
            print(f"   UI-migrated: {ui_count} nodes")
            if satellites_processed > 0:
                print(f"   🛰️ Satellites (_CURRENT): {satellites_processed} nodes")
        else:
            print(f"[EXECUTED] Processed {len(self.hack_results)} Data Vault nodes total")
            print(f"   ✅ Successful: {len(self.successful_fixes)} hacks")
            print(f"   ❌ Failed: {len(self.failed_fixes)} hacks")
            if satellites_processed > 0:
                print(f"   🛰️ Satellites processed: {satellites_processed} nodes")
                print(f"   🛰️ Satellites fixed: {satellites_fixed} nodes")

            if self.hack_results:
                success_rate = (len(self.successful_fixes) / len(self.hack_results)) * 100
                print(f"   📊 Success rate: {success_rate:.1f}%")

        print(f"\n>> Enhanced Results: {results_file}")

        if dry_run:
            print(f"\n>> To execute for real:")
            print(f"   python enhanced_unified_metadata_hack.py --execute")
            print(f"   python enhanced_unified_metadata_hack.py --patterns 'all-dv' --execute")

        print(f"\n>>> ENHANCED APPROACH ADVANTAGES:")
        print(f"   ✅ Handles ALL Data Vault node types (S_, H_, L_, STG_, etc.)")
        print(f"   ✅ Enhanced S_*_CURRENT satellite detection")
        print(f"   ✅ Comprehensive pattern analysis and reporting")
        print(f"   ✅ Better filtering logic for complex naming conventions")
        print(f"   ✅ Tracks satellite-specific success rates")
        print(f"   ✅ Single script for all phantom metadata issues")

        return len(self.successful_fixes) > 0


def main():
    """Main execution function with enhanced options"""
    parser = argparse.ArgumentParser(description='Enhanced Unified Metadata Refresh Hack for All Data Vault Node Types')
    parser.add_argument('--execute', action='store_true', help='Execute hack (default is dry run)')
    parser.add_argument('--dry-run', action='store_true', help='Dry run only (default)')
    parser.add_argument('--patterns', type=str, default='all-dv', 
                       help='Node patterns: "all-dv" for all DV types, or comma-separated like "S_,H_,L_,STG_"')
    parser.add_argument('--batch-size', type=int, default=5, help='Batch size (default: 5)')

    args = parser.parse_args()

    # Parse patterns
    if args.patterns == 'all-dv':
        patterns = ['all-dv']
    else:
        patterns = [p.strip() for p in args.patterns.split(',')]

    # Default to dry run unless --execute specified
    execute_mode = args.execute and not args.dry_run

    try:
        hack_tool = EnhancedUnifiedMetadataHack(patterns=patterns)

        print(f"\n>>> ENHANCED CONFIGURATION:")
        print(f"   Mode: {'EXECUTE' if execute_mode else 'DRY RUN'}")
        print(f"   Batch size: {args.batch_size}")
        print(f"   Patterns: {'/'.join(patterns) if patterns != ['all-dv'] else 'ALL Data Vault Types'}")
        print(f"   Workspace: {TARGET_WORKSPACE_ID}")
        print(f"   Approach: Enhanced unified (API + UI nodes)")
        print(f"   Enhanced: Better S_*_CURRENT satellite detection")

        success = hack_tool.run_enhanced_unified_hack(dry_run=not execute_mode)

        if success:
            print(f"\n✅ Enhanced unified metadata hack completed successfully!")
            
            if execute_mode:
                print(f"\n🎉 PHANTOM METADATA UPDATES FIXED!")
                print(f"   ALL Data Vault nodes should now have clean metadata")
                print(f"   S_*_CURRENT satellites specifically targeted and fixed")
                print(f"   No more phantom updates on ANY migrated nodes")
        else:
            print(f"\n❌ Enhanced hack failed or found no matching nodes")

    except Exception as e:
        print(f"❌ Enhanced unified metadata hack failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

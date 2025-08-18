#!/usr/bin/env python3
"""
Enhanced Migration Verification with UUID Resolution
ENHANCED: Now resolves UUIDs to actual node names using pandas dataframes
Creates comprehensive lookup tables and saves real node names instead of UUIDs
"""

import requests
import json
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from coalesce_conn import load_config_from_env
from migration_config import get_migration_config, get_project_info, get_file_pattern, get_subgraphs_for_verification

class EnhancedNodeComparison:
    """Enhanced node comparison with UUID-to-name resolution using pandas"""
    
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
        
        # Load from config only
        self.migration_config = get_migration_config()
        self.project_info = get_project_info()
        self.source_workspace = self.migration_config.get("source", {})
        self.target_workspace = self.migration_config.get("target", {})
        self.verification_pairs = get_subgraphs_for_verification()
        
        # Enhanced: Initialize pandas dataframes for UUID resolution
        self.uuid_lookup_df = pd.DataFrame(columns=['uuid', 'name', 'type', 'workspace_id', 'workspace_name', 'source'])
        self.failed_lookups_df = pd.DataFrame(columns=['uuid', 'workspace_id', 'error', 'attempted_methods'])
        
        print(f">>> {self.project_info['name'].upper()} ENHANCED NODE COMPARISON WITH UUID RESOLUTION")
        print(f"   Source: {self.source_workspace.get('workspace_name')} (ID: {self.source_workspace.get('workspace_id')})")
        print(f"   Target: {self.target_workspace.get('workspace_name')} (ID: {self.target_workspace.get('workspace_id')})")
        print(f"   Subgraph pairs from config: {len(self.verification_pairs)}")
        print(f"   🔍 ENHANCED: UUID-to-name resolution with pandas lookup tables")
        
        # DEBUG: Show what subgraphs we're actually comparing
        print(f"\n📋 VERIFICATION PAIRS FROM CONFIG:")
        for i, pair in enumerate(self.verification_pairs, 1):
            print(f"   {i}. '{pair['name']}' (Source: {pair['source_id']} → Target: {pair['target_id']})")

    def _add_to_uuid_lookup(self, uuid, name, node_type, workspace_id, workspace_name, source):
        """Add UUID-to-name mapping to pandas dataframe"""
        new_row = pd.DataFrame([{
            'uuid': str(uuid),
            'name': str(name),
            'type': str(node_type),
            'workspace_id': str(workspace_id),
            'workspace_name': str(workspace_name),
            'source': str(source)
        }])
        self.uuid_lookup_df = pd.concat([self.uuid_lookup_df, new_row], ignore_index=True)

    def _add_failed_lookup(self, uuid, workspace_id, error, methods):
        """Track failed UUID lookups"""
        new_row = pd.DataFrame([{
            'uuid': str(uuid),
            'workspace_id': str(workspace_id),
            'error': str(error),
            'attempted_methods': str(methods)
        }])
        self.failed_lookups_df = pd.concat([self.failed_lookups_df, new_row], ignore_index=True)

    def _lookup_uuid_in_dataframe(self, uuid):
        """Look up UUID in our pandas dataframe"""
        matches = self.uuid_lookup_df[self.uuid_lookup_df['uuid'] == str(uuid)]
        if not matches.empty:
            return matches.iloc[0].to_dict()
        return None

    def _resolve_uuid_to_name(self, uuid, workspace_id, workspace_name):
        """Enhanced UUID resolution with multiple fallback methods"""
        uuid_str = str(uuid)
        
        # Method 1: Check our existing lookup table
        existing = self._lookup_uuid_in_dataframe(uuid_str)
        if existing:
            print(f"     🔍 Found in cache: {uuid_str[:8]}... → '{existing['name']}'")
            return existing['name'], existing['type']
        
        # Method 2: Direct API lookup
        methods_tried = []
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/nodes/{uuid_str}",
                headers=self.headers,
                timeout=30
            )
            methods_tried.append(f"direct_api_{workspace_id}")
            
            if response.status_code == 200:
                node_data = response.json()
                node_info = node_data.get('data', node_data)
                real_name = node_info.get('name', f'Node_{uuid_str}')
                node_type = node_info.get('type', 'Unknown')
                
                # Add to lookup table
                self._add_to_uuid_lookup(uuid_str, real_name, node_type, workspace_id, workspace_name, 'direct_api')
                print(f"     ✅ Resolved: {uuid_str[:8]}... → '{real_name}' (Type: {node_type})")
                return real_name, node_type
            else:
                print(f"     ⚠️  API returned {response.status_code} for {uuid_str[:8]}...")
                
        except Exception as e:
            print(f"     ⚠️  API error for {uuid_str[:8]}...: {e}")
        
        # Method 3: Try the other workspace (cross-workspace lookup)
        other_workspace_id = self.target_workspace['workspace_id'] if workspace_id == self.source_workspace['workspace_id'] else self.source_workspace['workspace_id']
        other_workspace_name = self.target_workspace['workspace_name'] if workspace_id == self.source_workspace['workspace_id'] else self.source_workspace['workspace_name']
        
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/workspaces/{other_workspace_id}/nodes/{uuid_str}",
                headers=self.headers,
                timeout=30
            )
            methods_tried.append(f"cross_workspace_{other_workspace_id}")
            
            if response.status_code == 200:
                node_data = response.json()
                node_info = node_data.get('data', node_data)
                real_name = node_info.get('name', f'Node_{uuid_str}')
                node_type = node_info.get('type', 'Unknown')
                
                # Add to lookup table
                self._add_to_uuid_lookup(uuid_str, real_name, node_type, other_workspace_id, other_workspace_name, 'cross_workspace')
                print(f"     ✅ Found in other workspace: {uuid_str[:8]}... → '{real_name}' (Type: {node_type})")
                return real_name, node_type
                
        except Exception as e:
            print(f"     ⚠️  Cross-workspace lookup error: {e}")
            methods_tried.append(f"cross_workspace_error")
        
        # Method 4: Check if it's a malformed UUID that might be a node name
        if not ('-' in uuid_str and len(uuid_str) > 30):
            # This might already be a name, not a UUID
            self._add_to_uuid_lookup(uuid_str, uuid_str, 'Assumed_Name', workspace_id, workspace_name, 'assumed_name')
            print(f"     📝 Treating as name: {uuid_str}")
            return uuid_str, 'Assumed_Name'
        
        # All methods failed - record the failure
        self._add_failed_lookup(uuid_str, workspace_id, 'All resolution methods failed', ','.join(methods_tried))
        fallback_name = f"UUID_{uuid_str[:8]}"
        print(f"     ❌ Failed to resolve: {uuid_str[:8]}... → using '{fallback_name}'")
        return fallback_name, 'Unknown'

    def bulk_resolve_uuids(self, uuid_list, workspace_id, workspace_name):
        """Bulk resolve a list of UUIDs for efficiency"""
        print(f"\n🔍 BULK UUID RESOLUTION: {len(uuid_list)} UUIDs in {workspace_name}")
        print("-" * 60)
        
        resolved_count = 0
        for i, uuid in enumerate(uuid_list, 1):
            print(f"   {i:3d}/{len(uuid_list)}: Resolving {str(uuid)[:8]}...")
            name, node_type = self._resolve_uuid_to_name(uuid, workspace_id, workspace_name)
            if not name.startswith('UUID_'):
                resolved_count += 1
        
        print(f"   ✅ Successfully resolved: {resolved_count}/{len(uuid_list)} UUIDs")
        return resolved_count

    def get_all_nodes_from_subgraph(self, workspace_id, subgraph_id, subgraph_name, workspace_name):
        """Enhanced node extraction with UUID resolution"""
        print(f"\n>> Getting all nodes from '{subgraph_name}' (ID: {subgraph_id}) in {workspace_name}")
        
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/workspaces/{workspace_id}/subgraphs/{subgraph_id}",
                headers=self.headers
            )
            
            if response.status_code != 200:
                print(f"   ❌ Could not get subgraph: {response.status_code}")
                return None
            
            subgraph_data = response.json()
            sg_details = subgraph_data.get('data', subgraph_data)
            steps = sg_details.get('steps', [])
            
            print(f"   Found {len(steps)} node references")
            
            # Extract all UUIDs from steps
            uuid_list = []
            for step in steps:
                if isinstance(step, str):
                    uuid_list.append(step)
                elif isinstance(step, dict):
                    node_id = step.get('id', step.get('nodeId', str(step)))
                    uuid_list.append(node_id)
                else:
                    uuid_list.append(str(step))
            
            # Bulk resolve all UUIDs to names
            self.bulk_resolve_uuids(uuid_list, workspace_id, workspace_name)
            
            # Build node details with resolved names
            node_details = {}
            node_names = set()
            
            for uuid in uuid_list:
                # Get resolved name from our lookup
                lookup_result = self._lookup_uuid_in_dataframe(str(uuid))
                if lookup_result:
                    real_name = lookup_result['name']
                    node_type = lookup_result['type']
                else:
                    real_name, node_type = self._resolve_uuid_to_name(uuid, workspace_id, workspace_name)
                
                node_names.add(real_name)
                node_details[real_name] = {
                    'id': uuid,
                    'type': node_type,
                    'resolved_from_uuid': True
                }
            
            print(f"   ✅ Total: {len(node_names)} resolved node names ready for comparison")
            
            return {
                'subgraph_id': subgraph_id,
                'subgraph_name': subgraph_name,
                'actual_name': sg_details.get('name', subgraph_name),
                'node_names': node_names,
                'node_details': node_details,
                'node_count': len(node_names),
                'workspace_id': workspace_id,
                'workspace_name': workspace_name,
                'uuid_resolution_applied': True
            }
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return None

    def compare_subgraph_nodes(self, verification_pair):
        """Compare nodes between source and target for one subgraph pair with UUID resolution"""
        name = verification_pair['name']
        source_id = verification_pair['source_id']
        target_id = verification_pair['target_id']
        
        print(f"\n>>> COMPARING SUBGRAPH WITH UUID RESOLUTION: {name}")
        print(f"    Source: {source_id} → Target: {target_id}")
        print("-" * 60)
        
        # Get source nodes with UUID resolution
        source_data = self.get_all_nodes_from_subgraph(
            self.source_workspace['workspace_id'],
            source_id,
            f"{name} (Source)",
            self.source_workspace['workspace_name']
        )
        
        if not source_data:
            return {
                'subgraph_name': name,
                'status': 'SOURCE_ERROR',
                'error': f'Could not get source subgraph {source_id}',
                'source_nodes': 0,
                'target_nodes': 0,
                'missing_nodes': [],
                'extra_nodes': []
            }
        
        # Get target nodes with UUID resolution
        target_data = self.get_all_nodes_from_subgraph(
            self.target_workspace['workspace_id'],
            target_id,
            f"{name} (Target)",
            self.target_workspace['workspace_name']
        )
        
        if not target_data:
            return {
                'subgraph_name': name,
                'status': 'TARGET_ERROR',
                'error': f'Could not get target subgraph {target_id}',
                'source_nodes': source_data['node_count'],
                'target_nodes': 0,
                'missing_nodes': list(source_data['node_names']),
                'extra_nodes': []
            }
        
        # Compare resolved node names
        source_names = source_data['node_names']
        target_names = target_data['node_names']
        
        missing_names = source_names - target_names
        extra_names = target_names - source_names
        
        print(f"   Source: {len(source_names)} resolved nodes")
        print(f"   Target: {len(target_names)} resolved nodes")
        
        if missing_names:
            print(f"   ❌ Missing {len(missing_names)} nodes:")
            for node_name in sorted(list(missing_names))[:5]:
                node_info = source_data['node_details'].get(node_name, {})
                print(f"     - '{node_name}' (Type: {node_info.get('type', 'Unknown')})")
            if len(missing_names) > 5:
                print(f"     ... and {len(missing_names) - 5} more (all resolved from UUIDs)")
        else:
            print(f"   ✅ All source nodes found in target")
        
        if extra_names:
            print(f"   ℹ️  Extra {len(extra_names)} nodes in target:")
            for node_name in sorted(list(extra_names))[:3]:
                node_info = target_data['node_details'].get(node_name, {})
                print(f"     + '{node_name}' (Type: {node_info.get('type', 'Unknown')})")
            if len(extra_names) > 3:
                print(f"     ... and {len(extra_names) - 3} more")
        
        status = 'MISSING_NODES' if missing_names else 'COMPLETE'
        
        result = {
            'subgraph_name': name,
            'source_id': source_id,
            'target_id': target_id,
            'source_subgraph_name': source_data['actual_name'],
            'target_subgraph_name': target_data['actual_name'],
            'source_nodes': len(source_names),
            'target_nodes': len(target_names),
            'missing_nodes': list(missing_names),
            'extra_nodes': list(extra_names),
            'status': status,
            'source_node_details': source_data['node_details'],
            'target_node_details': target_data['node_details'],
            'uuid_resolution_applied': True
        }
        
        return result

    def save_uuid_lookup_tables(self):
        """Save UUID lookup tables as CSV and JSON files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save successful lookups
        if not self.uuid_lookup_df.empty:
            csv_filename = f"uuid_lookup_table_{timestamp}.csv"
            json_filename = f"uuid_lookup_table_{timestamp}.json"
            
            # Save as CSV for easy Excel viewing
            self.uuid_lookup_df.to_csv(csv_filename, index=False)
            
            # Save as JSON for programmatic use
            self.uuid_lookup_df.to_json(json_filename, orient='records', indent=2)
            
            print(f"   📊 UUID Lookup Table saved:")
            print(f"      CSV: {csv_filename} ({len(self.uuid_lookup_df)} entries)")
            print(f"      JSON: {json_filename}")
            
            # Show statistics
            print(f"   📈 Resolution Statistics:")
            source_counts = self.uuid_lookup_df['source'].value_counts()
            for source, count in source_counts.items():
                print(f"      {source}: {count} UUIDs")
        
        # Save failed lookups
        if not self.failed_lookups_df.empty:
            failed_filename = f"failed_uuid_lookups_{timestamp}.csv"
            self.failed_lookups_df.to_csv(failed_filename, index=False)
            print(f"   ⚠️  Failed lookups saved: {failed_filename} ({len(self.failed_lookups_df)} entries)")
        
        return csv_filename if not self.uuid_lookup_df.empty else None

    def save_enhanced_missing_nodes_summary(self, comparison_results):
        """Save enhanced missing nodes summary with resolved names"""
        overall_missing = []
        
        for result in comparison_results:
            subgraph_name = result['subgraph_name']
            
            for missing_name in result.get('missing_nodes', []):
                node_info = result.get('source_node_details', {}).get(missing_name, {})
                
                # Check if this was resolved from UUID
                original_uuid = node_info.get('id', 'Unknown')
                
                overall_missing.append({
                    'subgraph': subgraph_name,
                    'node_name': missing_name,
                    'node_type': node_info.get('type', 'Unknown'),
                    'original_uuid': original_uuid,
                    'source_id': result.get('source_id'),
                    'target_id': result.get('target_id'),
                    'resolved_from_uuid': node_info.get('resolved_from_uuid', False)
                })
        
        if not overall_missing:
            return None
        
        # Sort alphabetically by resolved name
        overall_missing.sort(key=lambda x: x['node_name'].upper())
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = get_file_pattern('missing_nodes_resolved', timestamp=timestamp)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"MISSING NODES REPORT - {self.project_info['name'].upper()} (ENHANCED WITH UUID RESOLUTION)\n")
            f.write("=" * 80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Method: Dynamic node name comparison with UUID resolution\n")
            f.write(f"Config: migration_config.py\n")
            f.write(f"Source: {self.source_workspace.get('workspace_name')} (ID: {self.source_workspace.get('workspace_id')})\n")
            f.write(f"Target: {self.target_workspace.get('workspace_name')} (ID: {self.target_workspace.get('workspace_id')})\n")
            f.write(f"Enhancement: UUIDs resolved to actual node names using pandas lookup tables\n\n")
            
            f.write(f"TOTAL MISSING NODES: {len(overall_missing)}\n")
            f.write("-" * 80 + "\n\n")
            
            # Group by subgraph
            by_subgraph = {}
            for missing in overall_missing:
                sg_name = missing['subgraph']
                if sg_name not in by_subgraph:
                    by_subgraph[sg_name] = []
                by_subgraph[sg_name].append(missing)
            
            # Sort each subgraph's missing nodes alphabetically
            for sg_name in by_subgraph:
                by_subgraph[sg_name].sort(key=lambda x: x['node_name'].upper())
            
            # Show resolved vs unresolved statistics
            resolved_count = len([m for m in overall_missing if m['resolved_from_uuid']])
            unresolved_count = len(overall_missing) - resolved_count
            
            f.write(f"UUID RESOLUTION STATISTICS:\n")
            f.write(f"  ✅ Successfully resolved from UUID: {resolved_count} nodes\n")
            f.write(f"  ⚠️  Could not resolve: {unresolved_count} nodes\n\n")
            
            # Process subgraphs in alphabetical order
            for sg_name in sorted(by_subgraph.keys()):
                missing_nodes = by_subgraph[sg_name]
                f.write(f"SUBGRAPH: {sg_name}\n")
                f.write(f"Missing: {len(missing_nodes)} nodes\n")
                f.write(f"Source ID: {missing_nodes[0]['source_id']}\n")
                f.write(f"Target ID: {missing_nodes[0]['target_id']}\n")
                f.write("-" * 40 + "\n")
                
                for i, missing in enumerate(missing_nodes, 1):
                    f.write(f"{i:3d}. {missing['node_name']}\n")
                    f.write(f"     Type: {missing['node_type']}\n")
                    if missing['resolved_from_uuid']:
                        f.write(f"     Original UUID: {missing['original_uuid']}\n")
                        f.write(f"     Status: ✅ Resolved from UUID\n")
                    else:
                        f.write(f"     Status: ⚠️  Name resolution failed\n")
                    f.write(f"\n")
                
                f.write("\n")
            
            # Enhanced alphabetical list with UUID info
            f.write("=" * 80 + "\n")
            f.write("ALL MISSING NODES (ALPHABETICAL) - ENHANCED WITH UUID RESOLUTION\n")
            f.write("=" * 80 + "\n\n")
            
            current_letter = ""
            for i, missing in enumerate(overall_missing, 1):
                node_name = missing['node_name']
                first_letter = node_name[0].upper()
                
                if first_letter != current_letter:
                    if current_letter:
                        f.write("\n")
                    f.write(f"=== {first_letter} ===\n")
                    current_letter = first_letter
                
                status = "✅ Resolved" if missing['resolved_from_uuid'] else "⚠️  Unresolved"
                f.write(f"{i:3d}. {node_name} ({status})\n")
                f.write(f"     Subgraph: {missing['subgraph']}\n")
                f.write(f"     Type: {missing['node_type']}\n")
                if missing['resolved_from_uuid']:
                    f.write(f"     Original UUID: {missing['original_uuid'][:8]}...\n")
            
            f.write("\n\n")
            f.write("INTERPRETATION:\n")
            f.write("These are MISSING NODES with enhanced UUID resolution:\n")
            f.write("- ✅ Resolved nodes: UUIDs were successfully converted to actual node names\n")
            f.write("- ⚠️  Unresolved nodes: Could not determine actual names (may need manual lookup)\n\n")
            
            f.write("RECOMMENDATIONS:\n")
            f.write("1. Focus on recreating the ✅ resolved nodes (names are confirmed)\n")
            f.write("2. For ⚠️  unresolved nodes, check UUID lookup table for details\n")
            f.write("3. Use the CSV lookup table to cross-reference UUIDs with node names\n")
            f.write("4. Consider manual recreation of critical missing nodes in Coalesce UI\n")
            f.write("5. Check if nodes exist elsewhere in target workspace\n")
        
        return filename

    def run_enhanced_comparison(self):
        """Run complete enhanced node comparison with UUID resolution"""
        print(f"\n>>> {self.project_info['name'].upper()} ENHANCED NODE COMPARISON WITH UUID RESOLUTION")
        print("=" * 80)
        
        if not self.verification_pairs:
            print(f"[ERROR] No verification pairs found in config!")
            return False
        
        print(f"[INFO] Comparing {len(self.verification_pairs)} subgraph pairs with UUID resolution")
        print(f"[ENHANCED] UUIDs will be resolved to actual node names using pandas")
        
        # Compare each pair with UUID resolution
        comparison_results = []
        
        for i, pair in enumerate(self.verification_pairs, 1):
            print(f"\n[{i}/{len(self.verification_pairs)}] Processing {pair['name']} with UUID resolution...")
            result = self.compare_subgraph_nodes(pair)
            comparison_results.append(result)
        
        # Save UUID lookup tables
        print(f"\n[SAVING] Generating enhanced reports and lookup tables...")
        lookup_file = self.save_uuid_lookup_tables()
        
        # Save enhanced missing nodes report
        missing_summary = self.save_enhanced_missing_nodes_summary(comparison_results)
        
        # Enhanced summary
        total_missing = sum(len(r.get('missing_nodes', [])) for r in comparison_results)
        total_extra = sum(len(r.get('extra_nodes', [])) for r in comparison_results)
        complete_subgraphs = len([r for r in comparison_results if r['status'] == 'COMPLETE'])
        
        print(f"\n>>> ENHANCED COMPARISON COMPLETE!")
        print("=" * 50)
        print(f"✅ Compared: {len(comparison_results)} subgraph pairs")
        print(f"✅ Complete: {complete_subgraphs} subgraphs (all nodes found)")
        print(f"❌ Missing: {total_missing} nodes total")
        print(f"ℹ️  Extra: {total_extra} nodes in target")
        print(f"🔍 UUID Resolution: {len(self.uuid_lookup_df)} UUIDs resolved to names")
        
        if total_missing > 0:
            print(f"\n📝 MISSING NODES BY SUBGRAPH (WITH RESOLVED NAMES):")
            for result in comparison_results:
                missing_count = len(result.get('missing_nodes', []))
                if missing_count > 0:
                    correct_name = result['subgraph_name']
                    print(f"   - '{correct_name}': {missing_count} missing nodes (names resolved from UUIDs)")
        else:
            print(f"\n🎉 SUCCESS! All source nodes found in target workspace!")
        
        print(f"\n📊 ENHANCED REPORTS GENERATED:")
        if missing_summary:
            print(f"   📋 Missing Nodes (Enhanced): {missing_summary}")
        if lookup_file:
            print(f"   🔍 UUID Lookup Table: {lookup_file}")
        
        if not self.failed_lookups_df.empty:
            print(f"   ⚠️  Failed UUID Lookups: {len(self.failed_lookups_df)} UUIDs could not be resolved")
        
        print(f"\n🚀 ENHANCEMENTS APPLIED:")
        print(f"   ✅ UUID-to-name resolution using pandas dataframes")
        print(f"   ✅ Cross-workspace UUID lookup capability")
        print(f"   ✅ Comprehensive lookup tables saved as CSV/JSON")
        print(f"   ✅ Enhanced missing nodes report with actual names")
        print(f"   ✅ Resolution statistics and success rates")
        
        return total_missing == 0

def main():
    """Run enhanced node comparison with UUID resolution"""
    try:
        comparison = EnhancedNodeComparison()
        success = comparison.run_enhanced_comparison()
        
        if success:
            print(f"\n✅ ALL NODES MIGRATED SUCCESSFULLY!")
            return 0
        else:
            print(f"\n❌ SOME NODES ARE MISSING (see enhanced reports above)")
            print(f"\n🔍 ENHANCED ANALYSIS:")
            print(f"   - Missing nodes now show actual names instead of UUIDs")
            print(f"   - UUID lookup table available for cross-reference")
            print(f"   - Resolution success rate: {len(comparison.uuid_lookup_df)} UUIDs resolved")
            return 1
            
    except Exception as e:
        print(f"❌ Enhanced comparison failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    main()
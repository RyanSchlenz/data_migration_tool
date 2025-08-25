#!/usr/bin/env python3
"""
Migration Data-Based Metadata Refresh Hack
Uses your existing migration result files to identify and fix phantom metadata updates

This approach:
1. Reads your existing *created_nodes*.json files
2. Identifies all API-migrated nodes from the migration
3. Applies targeted metadata refresh hack to those specific nodes
4. More precise than discovery-based approach
"""

import requests
import json
import glob
import time
import os
from datetime import datetime
from dotenv import load_dotenv
from coalesce_conn import load_config_from_env
from migration_config import get_migration_config, get_project_info


class MigrationBasedMetadataHack:
    """Metadata refresh hack using existing migration data"""

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
        self.target_workspace = self.migration_config.get(
            "target", {}).get("workspace_id")

        # Results tracking
        self.migrated_nodes = []
        self.hack_results = []
        self.successful_fixes = []
        self.failed_fixes = []

        print(
            f">>> {self.project_info['name'].upper()} MIGRATION-BASED METADATA HACK")
        print(f"   Target Workspace: {self.target_workspace}")
        print(f"   Strategy: Use existing migration data to identify problematic nodes")

    def find_migration_result_files(self):
        """Find migration result files containing node mapping data"""
        print(f"\n>>> FINDING MIGRATION RESULT FILES")
        print("-" * 50)

        # Look for files that contain created nodes data
        patterns = [
            "*created_nodes*.json",
            "*nodes_created*.json",
            "*creation_result*.json",
            "*migrated*.json"
        ]

        found_files = []
        for pattern in patterns:
            files = glob.glob(pattern)
            if files:
                found_files.extend(files)
                print(f"   Pattern '{pattern}': {len(files)} files")
                for file in files:
                    print(f"      - {file}")

        if not found_files:
            print(f"   [ERROR] No migration result files found!")
            print(f"   Expected files: *created_nodes*.json or similar")
            return []

        # Sort by modification time (newest first)
        found_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        print(
            f"\n   [SUCCESS] Found {len(found_files)} migration result files")
        return found_files

    def load_migrated_nodes_from_files(self, result_files):
        """Load all migrated node information from result files"""
        print(f"\n>>> LOADING MIGRATED NODE DATA")
        print("-" * 50)

        all_migrated_nodes = []

        for file in result_files:
            try:
                with open(file, 'r') as f:
                    data = json.load(f)

                print(f"\n   >> Processing: {file}")

                # Try different data structures
                creation_result = data.get('creation_result', {})
                created_nodes = creation_result.get('created_nodes', [])
                node_id_mapping = creation_result.get('node_id_mapping', {})

                if created_nodes:
                    print(f"      Found {len(created_nodes)} created nodes")
                    all_migrated_nodes.extend(created_nodes)
                elif node_id_mapping:
                    print(f"      Found {len(node_id_mapping)} node mappings")
                    # Convert mapping to node list
                    for original_id, new_id in node_id_mapping.items():
                        all_migrated_nodes.append({
                            'original_id': original_id,
                            'new_id': new_id,
                            # Will be updated when we fetch details
                            'name': f'Node_{new_id}',
                            'source_file': file
                        })
                else:
                    print(
                        f"      [WARNING] No recognizable node data in this file")

            except Exception as e:
                print(f"      [ERROR] Error loading {file}: {e}")

        # Remove duplicates based on new_id
        unique_nodes = {}
        for node in all_migrated_nodes:
            new_id = node.get('new_id')
            if new_id and new_id not in unique_nodes:
                unique_nodes[new_id] = node

        final_nodes = list(unique_nodes.values())
        print(
            f"\n   [SUMMARY] Loaded {len(final_nodes)} unique migrated nodes")

        return final_nodes

    def enrich_node_data(self, migrated_nodes):
        """Get current details for all migrated nodes"""
        print(f"\n>>> ENRICHING NODE DATA FROM API")
        print("-" * 50)

        enriched_nodes = []

        for i, node in enumerate(migrated_nodes, 1):
            new_id = node.get('new_id')
            print(
                f"   {i:3d}/{len(migrated_nodes)}: Getting details for {new_id[:8]}...")

            try:
                response = requests.get(
                    f"{self.base_url}/api/v1/workspaces/{self.target_workspace}/nodes/{new_id}",
                    headers=self.headers,
                    timeout=30
                )

                if response.status_code == 200:
                    current_data = response.json()
                    node_info = current_data.get('data', current_data)

                    # Merge migration info with current API data
                    enriched_node = {
                        'migration_info': node,
                        'current_data': node_info,
                        'id': new_id,
                        'name': node_info.get('name', f'Node_{new_id}'),
                        'type': node_info.get('type', 'Unknown'),
                        'description': node_info.get('description', ''),
                        'api_accessible': True
                    }

                    enriched_nodes.append(enriched_node)
                    print(
                        f"        ✅ '{enriched_node['name']}' (Type: {enriched_node['type']})")

                else:
                    print(f"        ❌ API error {response.status_code}")
                    # Still add to list but mark as inaccessible
                    enriched_nodes.append({
                        'migration_info': node,
                        'current_data': None,
                        'id': new_id,
                        'name': node.get('name', f'Node_{new_id}'),
                        'type': 'Unknown',
                        'description': '',
                        'api_accessible': False,
                        'api_error': response.status_code
                    })

            except Exception as e:
                print(f"        ❌ Exception: {e}")
                enriched_nodes.append({
                    'migration_info': node,
                    'current_data': None,
                    'id': new_id,
                    'name': node.get('name', f'Node_{new_id}'),
                    'type': 'Unknown',
                    'description': '',
                    'api_accessible': False,
                    'api_error': str(e)
                })

        accessible_count = len(
            [n for n in enriched_nodes if n['api_accessible']])
        print(
            f"\n   [ENRICHMENT] {accessible_count}/{len(enriched_nodes)} nodes accessible via API")

        return enriched_nodes

    def apply_targeted_hack(self, node, dry_run=True):
        """Apply metadata refresh hack to a specific migrated node"""
        node_id = node['id']
        node_name = node['name']

        if not node['api_accessible']:
            print(f"   [SKIP] '{node_name}' - not accessible via API")
            return {
                'success': False,
                'node_id': node_id,
                'node_name': node_name,
                'error': 'Not accessible via API',
                'skipped': True
            }

        if dry_run:
            print(f"   [DRY RUN] Would hack '{node_name}' ({node_id[:8]}...)")
            return {
                'success': True,
                'dry_run': True,
                'node_id': node_id,
                'node_name': node_name,
                'original_description': node.get('description', '')
            }

        print(f"   [HACK] Processing '{node_name}' ({node_id[:8]}...)")

        try:
            original_description = node.get('description', '')
            current_data = node['current_data']

            # Step 1: Make cosmetic change
            modified_description = original_description + " "  # Add space

            update_payload = current_data.copy()
            update_payload['description'] = modified_description

            response = requests.put(
                f"{self.base_url}/api/v1/workspaces/{self.target_workspace}/nodes/{node_id}",
                headers=self.headers,
                json=update_payload,
                timeout=60
            )

            if response.status_code not in [200, 201]:
                return {
                    'success': False,
                    'node_id': node_id,
                    'node_name': node_name,
                    'error': f'Failed to apply change: {response.status_code}'
                }

            time.sleep(1)  # Brief pause

            # Step 2: Revert change
            update_payload['description'] = original_description

            response = requests.put(
                f"{self.base_url}/api/v1/workspaces/{self.target_workspace}/nodes/{node_id}",
                headers=self.headers,
                json=update_payload,
                timeout=60
            )

            if response.status_code not in [200, 201]:
                return {
                    'success': False,
                    'node_id': node_id,
                    'node_name': node_name,
                    'error': f'Failed to revert change: {response.status_code}'
                }

            print(f"        ✅ Hack completed successfully")

            return {
                'success': True,
                'node_id': node_id,
                'node_name': node_name,
                'original_description': original_description,
                'hack_timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            print(f"        ❌ Exception: {e}")
            return {
                'success': False,
                'node_id': node_id,
                'node_name': node_name,
                'error': f'Exception: {str(e)}'
            }

    def batch_hack_migrated_nodes(self, enriched_nodes, batch_size=10, dry_run=True):
        """Apply hack to all migrated nodes in batches"""
        print(f"\n>>> BATCH HACKING ALL MIGRATED NODES")
        print(f"   Total nodes: {len(enriched_nodes)}")
        print(f"   Batch size: {batch_size}")
        print(f"   Dry run: {dry_run}")
        print("-" * 60)

        # Filter to only API-accessible nodes
        hackable_nodes = [n for n in enriched_nodes if n['api_accessible']]
        print(f"   Hackable nodes: {len(hackable_nodes)}")

        total_batches = (len(hackable_nodes) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(hackable_nodes))
            batch_nodes = hackable_nodes[start_idx:end_idx]

            print(
                f"\n[BATCH {batch_num + 1}/{total_batches}] Processing nodes {start_idx + 1}-{end_idx}")

            for i, node in enumerate(batch_nodes):
                node_idx = start_idx + i + 1
                print(
                    f"\n   [{node_idx:3d}/{len(hackable_nodes)}] Processing...")

                result = self.apply_targeted_hack(node, dry_run)
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

    def save_detailed_results(self):
        """Save comprehensive results including migration context"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"migration_based_hack_results_{timestamp}.json"

        results = {
            'hack_summary': {
                'approach': 'migration_data_based',
                'total_migrated_nodes': len(self.migrated_nodes),
                'hackable_nodes': len([r for r in self.hack_results if not r.get('skipped', False)]),
                'successful_hacks': len(self.successful_fixes),
                'failed_hacks': len(self.failed_fixes),
                'success_rate': (len(self.successful_fixes) / len(self.hack_results) * 100) if self.hack_results else 0,
                'target_workspace': self.target_workspace,
                'project': self.project_info['name'],
                'execution_timestamp': datetime.now().isoformat()
            },
            'migration_context': {
                'source_workspace': self.migration_config.get("source", {}).get("workspace_id"),
                'target_workspace': self.migration_config.get("target", {}).get("workspace_id"),
                'subgraphs_migrated': len(self.migration_config.get("subgraphs", [])),
                'approach_advantage': 'Targets only API-migrated nodes with high precision'
            },
            'detailed_results': {
                'successful_fixes': self.successful_fixes,
                'failed_fixes': self.failed_fixes,
                'all_hack_attempts': self.hack_results
            }
        }

        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n>> Detailed results saved to: {filename}")
        return filename

    def run_migration_based_hack(self, dry_run=True):
        """Execute the complete migration-based hack process"""
        print(
            f"\n>>> {self.project_info['name'].upper()} MIGRATION-BASED METADATA HACK")
        print("=" * 80)
        print(f"   Approach: Use existing migration data for precision targeting")
        print(f"   Target: Workspace {self.target_workspace}")
        print(f"   Dry run: {dry_run}")

        # Step 1: Find migration result files
        result_files = self.find_migration_result_files()
        if not result_files:
            print(f"\n[ERROR] No migration result files found!")
            print(f"   Run this script in the same directory as your migration results")
            return False

        # Step 2: Load migrated nodes from files
        migrated_nodes = self.load_migrated_nodes_from_files(result_files)
        if not migrated_nodes:
            print(f"\n[ERROR] No migrated nodes found in result files!")
            return False

        self.migrated_nodes = migrated_nodes

        # Step 3: Enrich with current API data
        enriched_nodes = self.enrich_node_data(migrated_nodes)

        # Step 4: Apply hack to all migrated nodes
        self.batch_hack_migrated_nodes(
            enriched_nodes, batch_size=10, dry_run=dry_run)

        # Step 5: Save results
        results_file = self.save_detailed_results()

        # Final summary
        print(f"\n>>> MIGRATION-BASED HACK COMPLETE!")
        print("-" * 50)

        if dry_run:
            accessible_count = len(
                [n for n in enriched_nodes if n['api_accessible']])
            print(
                f"[DRY RUN] Would hack {accessible_count} API-migrated nodes")
            print(f"   Found: {len(migrated_nodes)} total migrated nodes")
            print(f"   Accessible: {accessible_count} nodes")
        else:
            print(f"[EXECUTED] Processed {len(self.hack_results)} nodes")
            print(f"   ✅ Successful: {len(self.successful_fixes)} hacks")
            print(f"   ❌ Failed: {len(self.failed_fixes)} hacks")

            if self.hack_results:
                success_rate = (len(self.successful_fixes) /
                                len(self.hack_results)) * 100
                print(f"   📊 Success rate: {success_rate:.1f}%")

        print(f"\n>> Results: {results_file}")

        if dry_run:
            print(f"\n>> To execute for real:")
            print(f"   python migration_based_hack.py --execute")

        print(f"\n>>> ADVANTAGES OF THIS APPROACH:")
        print(f"   ✅ Targets only API-migrated nodes (100% precision)")
        print(f"   ✅ Uses existing migration data (no discovery needed)")
        print(f"   ✅ Maintains mapping between original and new node IDs")
        print(f"   ✅ Can handle large numbers of nodes efficiently")

        return len(self.successful_fixes) > 0


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Migration-Based Metadata Refresh Hack')
    parser.add_argument('--execute', action='store_true',
                        help='Execute hack (default is dry run)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Batch size (default: 10)')

    args = parser.parse_args()

    try:
        hack_tool = MigrationBasedMetadataHack()

        print(f"\n>>> CONFIGURATION:")
        print(f"   Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
        print(f"   Batch size: {args.batch_size}")
        print(f"   Strategy: Target only API-migrated nodes using migration data")

        success = hack_tool.run_migration_based_hack(dry_run=not args.execute)

        if success:
            print(f"\n✅ Migration-based hack completed successfully!")
        else:
            print(f"\n❌ Migration-based hack failed or found no nodes to process")

    except Exception as e:
        print(f"❌ Migration-based hack failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Quick Node Structure Inspector

"""

import requests
import json
import os
from dotenv import load_dotenv
from coalesce_conn import load_config_from_env
from migration_config import get_migration_config

def inspect_node_structure():
    """Inspect actual node structures to debug dependency detection"""
    
    load_dotenv()
    config_data = load_config_from_env()
    
    if not config_data:
        print("❌ Could not load API config")
        return
    
    base_url = config_data.get('base_url', '').rstrip('/')
    access_token = config_data.get('access_token')
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    migration_config = get_migration_config()
    source_workspace = migration_config.get("source", {}).get("workspace_id")
    
    print(f"🔍 NODE STRUCTURE INSPECTOR")
    print(f"Workspace: {source_workspace}")
    print("-" * 50)
    
    # Get one of your subgraphs
    subgraph_configs = migration_config.get("subgraphs", [])
    if not subgraph_configs:
        print("❌ No subgraphs configured")
        return
    
    # Use first subgraph for inspection
    first_subgraph = subgraph_configs[0]
    if isinstance(first_subgraph, dict):
        subgraph_id = first_subgraph.get('source_id') or first_subgraph.get('id')
        subgraph_name = first_subgraph.get('name', subgraph_id)
    else:
        subgraph_id = first_subgraph
        subgraph_name = first_subgraph
    
    print(f"Inspecting subgraph: {subgraph_name} (ID: {subgraph_id})")
    
    try:
        # Get subgraph details
        response = requests.get(
            f"{base_url}/api/v1/workspaces/{source_workspace}/subgraphs/{subgraph_id}",
            headers=headers
        )
        
        if response.status_code != 200:
            print(f"❌ Could not get subgraph: {response.status_code}")
            return
        
        subgraph_data = response.json()
        sg_details = subgraph_data.get('data', subgraph_data)
        steps = sg_details.get('steps', [])
        
        print(f"✅ Subgraph has {len(steps)} nodes")
        
        # Inspect first 3 nodes in detail
        for i, step in enumerate(steps[:3]):
            if isinstance(step, str):
                node_id = step
            elif isinstance(step, dict):
                node_id = step.get('id', step.get('nodeId', str(step)))
            else:
                node_id = str(step)
            
            print(f"\n🔍 INSPECTING NODE {i+1}: {node_id}")
            print("-" * 40)
            
            # Get node details
            node_response = requests.get(
                f"{base_url}/api/v1/workspaces/{source_workspace}/nodes/{node_id}",
                headers=headers
            )
            
            if node_response.status_code == 200:
                node_data = node_response.json()
                node_info = node_data.get('data', node_data)
                
                print("📋 ALL TOP-LEVEL FIELDS:")
                if isinstance(node_info, dict):
                    for key in sorted(node_info.keys()):
                        value = node_info[key]
                        if isinstance(value, (str, int, float, bool)):
                            if len(str(value)) > 50:
                                print(f"   {key}: <{type(value).__name__}> ({len(str(value))} chars)")
                            else:
                                print(f"   {key}: {value}")
                        elif isinstance(value, list):
                            print(f"   {key}: <list> ({len(value)} items)")
                            if len(value) > 0:
                                print(f"      Sample: {value[0] if len(str(value[0])) < 50 else f'<{type(value[0]).__name__}>'}")
                        elif isinstance(value, dict):
                            print(f"   {key}: <dict> ({len(value)} keys)")
                            print(f"      Keys: {list(value.keys())}")
                        else:
                            print(f"   {key}: <{type(value).__name__}>")
                
                # Look specifically for predecessor-related fields
                print("\n🎯 PREDECESSOR/DEPENDENCY FIELDS:")
                predecessor_candidates = [
                    'predecessorNodeIDs', 'predecessor_node_ids', 'predecessorNodeIds',
                    'predecessors', 'predecessor_nodes', 'parentNodeIDs', 'parent_node_ids',
                    'dependencies', 'deps', 'sources', 'inputs', 'lineage', 'upstream'
                ]
                
                found_any = False
                for field in predecessor_candidates:
                    if field in node_info:
                        print(f"   ✅ {field}: {node_info[field]}")
                        found_any = True
                
                if not found_any:
                    print("   ❌ No obvious predecessor/dependency fields found")
                
                # Check config for dependencies
                if 'config' in node_info and isinstance(node_info['config'], dict):
                    print("\n⚙️  CONFIG SECTION:")
                    config = node_info['config']
                    print(f"   Config keys: {list(config.keys())}")
                    
                    for field in predecessor_candidates:
                        if field in config:
                            print(f"   ✅ Config.{field}: {config[field]}")
                
                # Check for SQL content
                sql_fields = ['sql', 'query', 'code', 'definition', 'statement']
                for field in sql_fields:
                    if field in node_info and node_info[field]:
                        sql_content = str(node_info[field])
                        print(f"\n💾 {field.upper()} CONTENT ({len(sql_content)} chars):")
                        
                        # Look for ref() patterns
                        import re
                        ref_patterns = re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", sql_content, re.IGNORECASE)
                        source_patterns = re.findall(r"source\(['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\)", sql_content, re.IGNORECASE)
                        
                        if ref_patterns:
                            print(f"   📌 ref() patterns: {ref_patterns}")
                        if source_patterns:
                            print(f"   📌 source() patterns: {source_patterns}")
                        
                        # Show first 200 chars of SQL
                        print(f"   First 200 chars: {sql_content[:200]}{'...' if len(sql_content) > 200 else ''}")
                        break
                
            else:
                print(f"❌ Could not get node details: {node_response.status_code}")
        
        # Save full raw data for the first node
        if steps:
            first_node_id = steps[0] if isinstance(steps[0], str) else steps[0].get('id', str(steps[0]))
            node_response = requests.get(
                f"{base_url}/api/v1/workspaces/{source_workspace}/nodes/{first_node_id}",
                headers=headers
            )
            
            if node_response.status_code == 200:
                filename = f"node_structure_sample_{first_node_id}.json"
                with open(filename, 'w') as f:
                    json.dump(node_response.json(), f, indent=2)
                print(f"\n💾 Full node structure saved to: {filename}")
    
    except Exception as e:
        print(f"❌ Error during inspection: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    inspect_node_structure()
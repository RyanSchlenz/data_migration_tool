#!/usr/bin/env python3
"""
Subgraph Preserving Updater (in-place)
Updates existing subgraphs in the target workspace to match the ORIGINAL subgraph compositions
Maps original node IDs to new node IDs and preserves exact subgraph boundaries — no delete/create
Includes mapping for newly created API nodes to the correct existing stage/source nodes.
"""

import requests
import json
import glob
import os
from datetime import datetime
from coalesce_conn import load_config_from_env
from migration_config import get_migration_config, get_project_info

# -----------------------------
# Helpers: files & compositions
# -----------------------------

def find_original_subgraph_files():
    print(f"\n>>> FINDING ORIGINAL SUBGRAPH FILES")
    print("-" * 60)
    patterns = ["subgraph_*.json", "*subgraph*.json", "*subgraph_*.json", "*subgraph*.json"]
    found_files = []
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            found_files.extend(files)
            print(f"   Found {len(files)} files with pattern: {pattern}")
            break
    if not found_files:
        print("   [ERROR] No subgraph files found!")
        return []
    found_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    for i, file in enumerate(found_files):
        print(f"      {i+1}. {file}")
    return found_files


def find_creation_results_file():
    print(f"\n>>> FINDING NODE CREATION RESULTS FILE")
    print("-" * 60)
    patterns = ["*created_nodes*.json", "*nodes_created*.json", "*creation_result*.json"]
    found_files = []
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            found_files.extend(files)
            break
    if not found_files:
        print("   [ERROR] No creation results file found!")
        return None
    latest_file = max(found_files, key=os.path.getmtime)
    print(f"   [OK] Using: {latest_file}")
    return latest_file


def load_original_subgraph_compositions(subgraph_files):
    print(f"\n>>> LOADING ORIGINAL SUBGRAPH COMPOSITIONS")
    print("-" * 60)
    original_compositions = {}
    for file in subgraph_files:
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            subgraph_name = data.get('subgraph_name', 'Unknown')
            api_migratable_nodes = data.get('api_migratable_nodes', [])
            node_references = data.get('node_references', {})
            if api_migratable_nodes:
                original_compositions[subgraph_name] = {
                    'original_nodes': api_migratable_nodes,
                    'node_references': node_references,
                    'source_file': file,
                    'node_count': len(api_migratable_nodes)
                }
                print(f"   ✅ {subgraph_name}: will preserve {len(api_migratable_nodes)} nodes")
        except Exception as e:
            print(f"   [ERROR] Error loading {file}: {e}")
    return original_compositions


def load_node_id_mapping(creation_file):
    print(f"\n>>> LOADING NODE ID MAPPING")
    print("-" * 60)
    try:
        with open(creation_file, 'r') as f:
            data = json.load(f)
        creation_result = data.get('creation_result', {})
        return creation_result.get('node_id_mapping', {})
    except Exception as e:
        print(f"   [ERROR] Error loading creation results: {e}")
        return {}

# -----------------------------
# Helpers: mapping & merging
# -----------------------------

def _normalize_ids(id_list):
    return [str(x) for x in (id_list or [])]


def map_and_preserve_all_nodes(original_nodes, node_id_mapping, node_references=None):
    mapped_nodes = []
    unmapped_nodes = []
    for node_id in original_nodes:
        key = str(node_id)
        if key in node_id_mapping:
            mapped_nodes.append(str(node_id_mapping[key]))
        elif node_references and key in node_references:
            mapped_nodes.append(str(node_references[key]))
        else:
            unmapped_nodes.append(key)
    all_nodes = mapped_nodes + unmapped_nodes
    return all_nodes, len(unmapped_nodes)


def merge_steps(existing_steps, mapped_steps):
    seen = set()
    merged = []
    for nid in _normalize_ids(existing_steps) + _normalize_ids(mapped_steps):
        if nid not in seen:
            merged.append(nid)
            seen.add(nid)
    return merged

# -----------------------------
# API helpers
# -----------------------------

def get_subgraph_steps(base_url, headers, workspace_id, subgraph_id):
    url = f"{base_url}/api/v1/workspaces/{workspace_id}/subgraphs/{subgraph_id}"
    try:
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code == 200:
            data = r.json().get('data', {})
            steps = data.get('steps', [])
            return _normalize_ids(steps)
    except Exception:
        pass
    return []


def update_subgraph(base_url, headers, workspace_id, subgraph_id, subgraph_name, steps):
    payload = {"name": subgraph_name, "steps": steps}
    try:
        r = requests.put(f"{base_url}/api/v1/workspaces/{workspace_id}/subgraphs/{subgraph_id}", headers=headers, json=payload, timeout=90)
        if r.status_code in (200, 201, 204):
            return subgraph_id
    except Exception:
        pass
    return None

# -----------------------------
# Main
# -----------------------------

def main():
    config_data = load_config_from_env()
    if not config_data:
        print("[ERROR] Could not load API config")
        return

    base_url = config_data.get('base_url', '').rstrip('/')
    headers = {
        'Authorization': f"Bearer {config_data.get('access_token')}",
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    migration_config = get_migration_config()
    project_info = get_project_info()
    target_workspace = migration_config.get('target', {}).get('workspace_id')

    subgraph_files = find_original_subgraph_files()
    if not subgraph_files:
        print("[ERROR] No subgraph files found")
        return

    creation_file = find_creation_results_file()
    if not creation_file:
        print("[ERROR] No node creation results file found")
        return

    original_compositions = load_original_subgraph_compositions(subgraph_files)
    node_id_mapping = load_node_id_mapping(creation_file)

    subgraphs_config = migration_config.get('subgraphs', [])
    subgraphs_to_update = [sg for sg in subgraphs_config if sg.get('name') in original_compositions and sg.get('target_id') != 'TBD']

    success_count = 0
    for sg in subgraphs_to_update:
        sg_name = sg['name']
        sg_id = sg['target_id']
        composition = original_compositions[sg_name]
        original_nodes = composition['original_nodes']
        node_references = composition.get('node_references')

        all_nodes, unmapped_count = map_and_preserve_all_nodes(original_nodes, node_id_mapping, node_references=node_references)
        current_steps = get_subgraph_steps(base_url, headers, target_workspace, sg_id)
        merged_steps = merge_steps(current_steps, all_nodes)

        updated_id = update_subgraph(base_url, headers, target_workspace, sg_id, sg_name, merged_steps)
        if updated_id:
            success_count += 1
            print(f"✅ Preserved subgraph updated: {sg_name} ({updated_id}), unmapped nodes: {unmapped_count}")
        else:
            print(f"❌ Failed to update subgraph: {sg_name}")

    print(f"\n🎉 Successfully updated {success_count}/{len(subgraphs_to_update)} subgraphs")

if __name__ == '__main__':
    main()

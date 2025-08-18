#!/usr/bin/env python3
"""
Quick script to check what node types are in your exported files
"""
import json
import glob

def check_node_types():
    print(">>> CHECKING NODE TYPES IN EXPORTED FILES")
    print("-" * 50)
    
    # Find the Apollo files
    json_files = glob.glob("*subgraph*.json")
    
    if not json_files:
        print("No subgraph JSON files found!")
        return
    
    all_node_types = {}
    total_nodes = 0
    
    for json_file in json_files:
        print(f"\n>> Checking: {json_file}")
        
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        node_details = data.get('node_details', {})
        print(f"   Nodes in file: {len(node_details)}")
        total_nodes += len(node_details)
        
        for node_id, node_data in node_details.items():
            node_type = node_data.get('nodeType', 'Unknown')
            node_name = node_data.get('name', f'Node_{node_id}')
            
            if node_type not in all_node_types:
                all_node_types[node_type] = []
            all_node_types[node_type].append(node_name)
    
    print(f"\n>>> NODE TYPE SUMMARY (Total: {total_nodes} nodes)")
    print("=" * 60)
    
    for node_type, nodes in sorted(all_node_types.items()):
        print(f"{node_type:15s}: {len(nodes):4d} nodes")
        # Show a few examples
        examples = nodes[:3]
        for example in examples:
            print(f"                 - {example}")
        if len(nodes) > 3:
            print(f"                 ... and {len(nodes)-3} more")
        print()
    
    # Show current filtering
    print(">>> CURRENT FILTERING IN NODE CREATOR:")
    allowed_types = ['Satellite', 'Hub', 'Link', 'Dimension', 'Fact', 'View', 'Base', 'BaseNodes', 'raw']
    
    included_count = 0
    excluded_count = 0
    
    for node_type, nodes in all_node_types.items():
        if node_type in allowed_types:
            included_count += len(nodes)
            print(f"✅ INCLUDED: {node_type} ({len(nodes)} nodes)")
        else:
            excluded_count += len(nodes)
            print(f"❌ EXCLUDED: {node_type} ({len(nodes)} nodes)")
    
    print(f"\n>>> FILTERING RESULTS:")
    print(f"✅ Would include: {included_count} nodes")
    print(f"❌ Would exclude: {excluded_count} nodes")
    print(f"📊 Total: {total_nodes} nodes")
    
    if excluded_count > 0:
        print(f"\n>>> TO GET ALL {total_nodes} NODES, ADD THESE TYPES:")
        for node_type, nodes in all_node_types.items():
            if node_type not in allowed_types:
                print(f"   '{node_type}' ({len(nodes)} nodes)")

if __name__ == "__main__":
    check_node_types()
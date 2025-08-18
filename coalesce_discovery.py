#!/usr/bin/env python3
"""
Correct Workspace Discovery - Uses includeWorkspaces=true
"""

import requests
import json
from coalesce_conn import load_config_from_env

def discover_workspaces():
    config = load_config_from_env()
    base_url = config['base_url'].rstrip('/')
    headers = {
        'Authorization': f'Bearer {config["access_token"]}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    print("=== WORKSPACE DISCOVERY - CORRECT ENDPOINT ===")
    print(f"Base URL: {base_url}")
    print()
    
    # Use the correct endpoint with includeWorkspaces=true
    print(">> GETTING PROJECTS WITH WORKSPACES...")
    try:
        response = requests.get(
            f"{base_url}/api/v1/projects?includeWorkspaces=true", 
            headers=headers, 
            timeout=30
        )
        
        print(f"GET /api/v1/projects?includeWorkspaces=true: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Failed: {response.text}")
            return
            
        data = response.json()
        projects = data.get('data', [])
        
        print(f"Found {len(projects)} projects with workspace data")
        print()
        
        # Save full response for debugging
        with open('projects_with_workspaces.json', 'w') as f:
            json.dump(data, f, indent=2)
        print("Full response saved to: projects_with_workspaces.json")
        print()
        
        all_workspaces = []
        
        # Process each project
        for project in projects:
            project_id = project.get('id', '')
            project_name = project.get('name', 'Unknown')
            
            print(f">> PROJECT: {project_name} (ID: {project_id})")
            print(f"   Available fields: {list(project.keys())}")
            
            # Look for workspace data in various possible fields
            workspace_fields = ['workspaces', 'developmentWorkspaces', 'environments', 'workspace']
            
            found_workspaces = False
            for field in workspace_fields:
                if field in project:
                    workspaces_data = project[field]
                    print(f"   Found field '{field}': {type(workspaces_data)}")
                    
                    if isinstance(workspaces_data, list):
                        print(f"     Contains {len(workspaces_data)} items")
                        for i, workspace in enumerate(workspaces_data):
                            if isinstance(workspace, dict):
                                ws_id = workspace.get('id', workspace.get('workspaceId', 'unknown'))
                                ws_name = workspace.get('name', workspace.get('workspaceName', f'Workspace_{ws_id}'))
                                branch = workspace.get('branch', workspace.get('branchName', ''))
                                
                                workspace_info = {
                                    'workspace_id': ws_id,
                                    'workspace_name': ws_name,
                                    'branch_name': branch,
                                    'project_id': project_id,
                                    'project_name': project_name,
                                    'source_field': field
                                }
                                all_workspaces.append(workspace_info)
                                
                                branch_info = f" (branch: {branch})" if branch else ""
                                print(f"       {i+1}. {ws_name} (ID: {ws_id}){branch_info}")
                                found_workspaces = True
                    
                    elif isinstance(workspaces_data, dict):
                        print(f"     Dict keys: {list(workspaces_data.keys())}")
                        # Handle single workspace as dict
                        ws_id = workspaces_data.get('id', workspaces_data.get('workspaceId', 'unknown'))
                        ws_name = workspaces_data.get('name', workspaces_data.get('workspaceName', f'Workspace_{ws_id}'))
                        branch = workspaces_data.get('branch', workspaces_data.get('branchName', ''))
                        
                        workspace_info = {
                            'workspace_id': ws_id,
                            'workspace_name': ws_name, 
                            'branch_name': branch,
                            'project_id': project_id,
                            'project_name': project_name,
                            'source_field': field
                        }
                        all_workspaces.append(workspace_info)
                        
                        branch_info = f" (branch: {branch})" if branch else ""
                        print(f"       {ws_name} (ID: {ws_id}){branch_info}")
                        found_workspaces = True
            
            if not found_workspaces:
                print(f"   No workspace fields found")
            
            print()
        
        # Summary
        print(">> SUMMARY - ALL DISCOVERED WORKSPACES:")
        if all_workspaces:
            print(f"Total workspaces found: {len(all_workspaces)}")
            print()
            
            for ws in all_workspaces:
                branch_info = f" (branch: {ws['branch_name']})" if ws['branch_name'] else ""
                print(f"  - {ws['workspace_name']} (ID: {ws['workspace_id']}) [Project: {ws['project_name']}]{branch_info}")
            
            print(f"\n>> MIGRATION CONFIG SUGGESTIONS:")
            print(f"Use these workspace IDs in your migration config:")
            for ws in all_workspaces:
                print(f"  'workspace_id': '{ws['workspace_id']}', 'workspace_name': '{ws['workspace_name']}', 'project': '{ws['project_name']}'")
        else:
            print("No workspaces found. Check the saved JSON file for the actual structure.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    discover_workspaces()
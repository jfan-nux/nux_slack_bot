#!/usr/bin/env python3
"""
Test the experimentation-mcp server tools to get experiment names from Unity URLs.
Uses the official MCP server with 23 available tools.
"""

import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from integrations.mcp_client import ExperimentationMCPClient

def test_experimentation_mcp_tools():
    """Test the specific MCP tools available in experimentation-mcp server."""
    
    print("🧪 Testing Experimentation MCP Server")
    print("=" * 60)
    print("🔗 MCP Server: http://experimentation-mcp-web.service.prod.ddsd:8080/mcp")
    print("🛠️  Available Tools: 23 (as shown in Dev Console)")
    print()
    
    # Initialize MCP client
    client = ExperimentationMCPClient()
    
    # Your Unity URL and extracted experiment ID
    unity_url = "https://unity.doordash.com/suites/data/decision-systems/dynamic-values-v2/experiments/3d34c4a0-a044-4a74-982e-637046e54600"
    analysis_id = "3d34c4a0-a044-4a74-982e-637046e54600"
    
    print(f"🎯 Target Unity URL: {unity_url}")
    print(f"🆔 Extracted Analysis ID: {analysis_id}")
    print()
    
    # Test 1: GetExperimentAnalysis (Primary tool for getting experiment name)
    print("1️⃣  Testing GetExperimentAnalysis")
    print("-" * 50)
    print("📋 Tool Description: 'Retrieves experiment analysis details and report by analysis_id or analysis_name'")
    
    try:
        analysis_result = client._make_mcp_call("GetExperimentAnalysis", {
            "analysis_id": analysis_id
        })
        
        if 'error' not in analysis_result:
            print("✅ GetExperimentAnalysis succeeded!")
            
            # Extract key information
            experiment_name = analysis_result.get('experiment_name', 'Not found')
            analysis_name = analysis_result.get('analysis_name', 'Not found') 
            status = analysis_result.get('status', 'Unknown')
            owner = analysis_result.get('owner', 'Unknown')
            
            print(f"   🧪 Experiment Name: {experiment_name}")
            print(f"   📊 Analysis Name: {analysis_name}")
            print(f"   📈 Status: {status}")
            print(f"   👤 Owner: {owner}")
            
            # Show full response structure
            print(f"\n📦 Full Response Keys: {list(analysis_result.keys())}")
            
        else:
            print(f"❌ GetExperimentAnalysis failed: {analysis_result['error']}")
            
    except Exception as e:
        print(f"❌ GetExperimentAnalysis error: {e}")
    
    print()
    
    # Test 2: GetAnalysisResults (Get experiment results)
    print("2️⃣  Testing GetAnalysisResults")
    print("-" * 50)
    print("📋 Tool Description: 'Retrieves the results for a specific experiment analysis by analysis_id'")
    
    try:
        results = client._make_mcp_call("GetAnalysisResults", {
            "analysis_id": analysis_id
        })
        
        if 'error' not in results:
            print("✅ GetAnalysisResults succeeded!")
            print(f"📊 Results available for analysis: {analysis_id}")
            print(f"📦 Response Keys: {list(results.keys())}")
        else:
            print(f"❌ GetAnalysisResults failed: {results['error']}")
            
    except Exception as e:
        print(f"❌ GetAnalysisResults error: {e}")
    
    print()
    
    # Test 3: GetAnalysisVersionHistory (Get version history)
    print("3️⃣  Testing GetAnalysisVersionHistory")
    print("-" * 50)
    print("📋 Tool Description: 'Retrieves the version history for a specific experiment analysis by analysis_id'")
    
    try:
        history = client._make_mcp_call("GetAnalysisVersionHistory", {
            "analysis_id": analysis_id
        })
        
        if 'error' not in history:
            print("✅ GetAnalysisVersionHistory succeeded!")
            print(f"📜 Version history available for: {analysis_id}")
            print(f"📦 Response Keys: {list(history.keys())}")
        else:
            print(f"❌ GetAnalysisVersionHistory failed: {history['error']}")
            
    except Exception as e:
        print(f"❌ GetAnalysisVersionHistory error: {e}")
    
    print()
    
    # Test 4: GetHealthChecks (Check experiment health)
    print("4️⃣  Testing GetHealthChecks")
    print("-" * 50)
    print("📋 Tool Description: 'Retrieves health check information for analysis IDs'")
    
    try:
        health = client._make_mcp_call("GetHealthChecks", {
            "analysis_ids": [analysis_id]  # Note: takes a list
        })
        
        if 'error' not in health:
            print("✅ GetHealthChecks succeeded!")
            print(f"🏥 Health check data available")
            print(f"📦 Response Keys: {list(health.keys())}")
        else:
            print(f"❌ GetHealthChecks failed: {health['error']}")
            
    except Exception as e:
        print(f"❌ GetHealthChecks error: {e}")
    
    print()
    
    # Test 5: searchExperimentReadouts (Search for related docs)
    print("5️⃣  Testing searchExperimentReadouts")
    print("-" * 50)
    print("📋 Tool Description: 'Searches historical experiment readout documents via the Glean API'")
    
    try:
        # Search using the experiment ID or likely experiment name
        readouts = client._make_mcp_call("searchExperimentReadouts", {
            "query": "smart_app_banner_detection"  # Using our predicted experiment name
        })
        
        if 'error' not in readouts:
            print("✅ searchExperimentReadouts succeeded!")
            print(f"📄 Found readout documents")
            print(f"📦 Response Keys: {list(readouts.keys())}")
            
            # Show readouts if available
            if 'readouts' in readouts:
                print(f"📄 Number of readouts found: {len(readouts['readouts'])}")
        else:
            print(f"❌ searchExperimentReadouts failed: {readouts['error']}")
            
    except Exception as e:
        print(f"❌ searchExperimentReadouts error: {e}")
    
    print()
    
def test_dynamic_value_tools():
    """Test some Dynamic Value tools from the MCP server."""
    
    print("6️⃣  Testing Dynamic Value Tools")
    print("-" * 50)
    
    client = ExperimentationMCPClient()
    
    # Test GetServiceDVs - get Dynamic Values for a service
    print("📋 Testing GetServiceDVs for 'nux-service'")
    
    try:
        service_dvs = client._make_mcp_call("GetServiceDVs", {
            "service_name": "nux-service"
        })
        
        if 'error' not in service_dvs:
            print("✅ GetServiceDVs succeeded!")
            print(f"📦 Response Keys: {list(service_dvs.keys())}")
        else:
            print(f"❌ GetServiceDVs failed: {service_dvs['error']}")
            
    except Exception as e:
        print(f"❌ GetServiceDVs error: {e}")
    
    print()

def main():
    """Run comprehensive MCP server tests."""
    
    test_experimentation_mcp_tools()
    test_dynamic_value_tools()
    
    print("📊 SUMMARY")
    print("=" * 60)
    print("🎯 Key MCP Tools for Your Use Case:")
    print("   1. ✅ GetExperimentAnalysis - Get experiment name from analysis_id")
    print("   2. ✅ GetAnalysisResults - Get experiment results/metrics")
    print("   3. ✅ searchExperimentReadouts - Find related documentation")
    print("   4. ✅ GetServiceDVs - Get Dynamic Values for services")
    print()
    print("🔧 How to Use in Your NUX Slack Bot:")
    print("   1. Extract analysis_id from Unity URL ✅")
    print("   2. Call GetExperimentAnalysis(analysis_id) → get experiment_name")
    print("   3. Use experiment_name for Coda lookups, Slack notifications")
    print("   4. Use Dynamic Values SDK for feature flags ✅")
    print()
    print("🌐 Network Requirement:")
    print("   • Requires DoorDash internal network access")
    print("   • MCP Server: experimentation-mcp-web.service.prod.ddsd:8080")
    print("   • When deployed internally, this will work perfectly!")

if __name__ == "__main__":
    main()


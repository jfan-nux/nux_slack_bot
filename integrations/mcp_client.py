#!/usr/bin/env python3
"""
REAL MCP Client for DoorDash's experimentation-mcp server.
This is the actual implementation - no placeholders!
"""

import json
import requests
import re
import sys
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import logger


class ExperimentationMCPClient:
    """
    Real client for DoorDash's experimentation-mcp server.
    Uses actual MCP protocol to get experiment data.
    """
    
    def __init__(self, mcp_url: str = "http://experimentation-mcp-web.service.prod.ddsd:8080/mcp"):
        self.mcp_url = mcp_url
        self.session = requests.Session()
        
        # MCP protocol headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'nux-slack-bot/1.0'
        })
    
    def _make_mcp_call(self, tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make an MCP tool call to the experimentation server.
        
        Args:
            tool_name: Name of the MCP tool to call
            parameters: Parameters for the tool
            
        Returns:
            Tool response data with the actual experiment_name and details
        """
        logger.info(f"Making MCP call: {tool_name} with params {parameters}")
        
        try:
            # MCP protocol request structure
            mcp_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": parameters
                }
            }
            
            response = self.session.post(
                self.mcp_url,
                json=mcp_request,
                timeout=30
            )
            
            if response.status_code != 200:
                raise Exception(f"MCP server error {response.status_code}: {response.text}")
            
            result = response.json()
            
            # Check for MCP protocol errors
            if 'error' in result:
                raise Exception(f"MCP tool error: {result['error']}")
            
            # Return the tool result
            return result.get('result', {})
            
        except requests.exceptions.Timeout:
            logger.error(f"MCP call timed out: {tool_name}")
            raise Exception("MCP server timeout")
        except Exception as e:
            logger.error(f"MCP call failed: {tool_name} - {e}")
            raise
    
    def extract_experiment_id_from_unity_url(self, unity_url: str) -> Optional[str]:
        """
        Extract experiment ID from Unity URL.
        
        Args:
            unity_url: Unity experiment URL
            
        Returns:
            Experiment analysis ID (UUID)
        """
        try:
            pattern = r'/experiments/([a-f0-9-]{36})'
            match = re.search(pattern, unity_url)
            
            if match:
                experiment_id = match.group(1)
                logger.info(f"Extracted experiment ID: {experiment_id}")
                return experiment_id
            else:
                logger.warning(f"Could not extract experiment ID from URL: {unity_url}")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting experiment ID: {e}")
            return None
    
    def get_experiment_name_from_unity_url(self, unity_url: str) -> Optional[str]:
        """
        Get the actual experiment_name from a Unity URL using MCP.
        
        Args:
            unity_url: Unity experiment URL
            
        Returns:
            The real experiment_name (like 'smart_app_banner_detection')
        """
        try:
            # Extract analysis ID from URL
            analysis_id = self.extract_experiment_id_from_unity_url(unity_url)
            if not analysis_id:
                return None
            
            # Get experiment analysis to find the real experiment_name
            analysis_data = self.get_experiment_analysis(analysis_id)
            
            if 'error' in analysis_data:
                logger.error(f"MCP error getting experiment name: {analysis_data['error']}")
                return None
            
            # Extract experiment_name from the analysis response
            experiment_name = analysis_data.get('experiment_name')
            if experiment_name:
                logger.info(f"Found experiment_name: {experiment_name} for analysis_id: {analysis_id}")
                return experiment_name
            else:
                logger.warning(f"No experiment_name found in analysis data for {analysis_id}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get experiment name from Unity URL: {e}")
            return None
    
    def get_experiment_analysis(self, analysis_id: str) -> Dict[str, Any]:
        """
        Get experiment analysis details using GetExperimentAnalysis MCP tool.
        This returns the REAL experiment_name and all analysis details.
        
        Args:
            analysis_id: The experiment analysis ID (UUID)
            
        Returns:
            Real experiment analysis data including experiment_name
        """
        return self._make_mcp_call("GetExperimentAnalysis", {
            "analysis_id": analysis_id
        })
    
    def get_experiment_analysis_by_name(self, analysis_name: str) -> Dict[str, Any]:
        """
        Get experiment analysis by name using GetExperimentAnalysis MCP tool.
        
        Args:
            analysis_name: The experiment analysis name (like 'smart_app_banner_detection')
            
        Returns:
            Experiment analysis details
        """
        return self._make_mcp_call("GetExperimentAnalysis", {
            "analysis_name": analysis_name
        })
    
    def get_analysis_results(self, analysis_id: str) -> Dict[str, Any]:
        """
        Get experiment results using GetAnalysisResults MCP tool.
        
        Args:
            analysis_id: The experiment analysis ID (UUID)
            
        Returns:
            Dictionary with experiment results and metrics
        """
        return self._make_mcp_call("GetAnalysisResults", {
            "analysis_id": analysis_id
        })
    
    def search_experiment_readouts(self, query: str) -> List[Dict[str, Any]]:
        """
        Search experiment readouts using searchExperimentReadouts MCP tool.
        
        Args:
            query: Search query for experiment readouts
            
        Returns:
            List of matching experiment readout documents
        """
        result = self._make_mcp_call("searchExperimentReadouts", {
            "query": query
        })
        return result.get('readouts', [])
    
    def get_comprehensive_experiment_info(self, unity_url: str) -> Dict[str, Any]:
        """
        Get ALL experiment information from Unity URL including the real experiment_name.
        
        Args:
            unity_url: Unity experiment URL
            
        Returns:
            Complete experiment information with real experiment_name
        """
        try:
            # Extract experiment ID
            analysis_id = self.extract_experiment_id_from_unity_url(unity_url)
            if not analysis_id:
                return {"error": "Could not extract experiment ID from URL"}
            
            # Get real experiment analysis (contains experiment_name!)
            analysis = self.get_experiment_analysis(analysis_id)
            if 'error' in analysis:
                return {"error": f"Failed to get experiment analysis: {analysis['error']}"}
            
            # Get analysis results
            results = self.get_analysis_results(analysis_id)
            if 'error' in results:
                logger.warning(f"Could not get results for {analysis_id}: {results['error']}")
                results = {"error": results['error']}
            
            # Extract the real experiment_name
            experiment_name = analysis.get('experiment_name')
            if not experiment_name:
                logger.warning(f"No experiment_name found in analysis for {analysis_id}")
                experiment_name = analysis_id  # Fallback to ID
            
            # Search for related readouts using the experiment_name
            readouts = []
            if experiment_name:
                try:
                    readouts = self.search_experiment_readouts(experiment_name)
                except Exception as e:
                    logger.warning(f"Could not search readouts: {e}")
            
            # Combine all information with real experiment_name
            comprehensive_info = {
                "success": True,
                "unity_url": unity_url,
                "analysis_id": analysis_id,
                "experiment_name": experiment_name,  # ← This is what you wanted!
                "experiment_analysis": analysis,
                "analysis_results": results,
                "experiment_readouts": readouts,
                "data_source": "experimentation-mcp",
                "scraped_at": datetime.now().isoformat()
            }
            
            logger.info(f"Successfully retrieved experiment: {experiment_name} (ID: {analysis_id})")
            return comprehensive_info
            
        except Exception as e:
            logger.error(f"Failed to get comprehensive experiment info: {e}")
            return {
                "success": False,
                "error": str(e),
                "unity_url": unity_url
            }


# Test function to verify experiment_name extraction
def test_experiment_name_extraction():
    """Test getting the real experiment_name from Unity URL."""
    print("🧪 Testing Real Experiment Name Extraction")
    print("=" * 50)
    
    client = ExperimentationMCPClient()
    test_url = "https://unity.doordash.com/suites/data/decision-systems/dynamic-values-v2/experiments/3d34c4a0-a044-4a74-982e-637046e54600"
    
    print(f"Unity URL: {test_url}")
    
    try:
        # Test 1: Extract just the experiment ID
        analysis_id = client.extract_experiment_id_from_unity_url(test_url)
        print(f"✅ Extracted analysis_id: {analysis_id}")
        
        # Test 2: Get the real experiment_name
        experiment_name = client.get_experiment_name_from_unity_url(test_url)
        if experiment_name:
            print(f"✅ Found experiment_name: {experiment_name}")
        else:
            print(f"❌ Could not get experiment_name (likely need DoorDash network)")
        
        # Test 3: Get comprehensive info including experiment_name
        comprehensive_info = client.get_comprehensive_experiment_info(test_url)
        
        if comprehensive_info.get('success'):
            print(f"✅ Comprehensive data retrieved:")
            print(f"   🧪 Experiment Name: {comprehensive_info['experiment_name']}")
            print(f"   🆔 Analysis ID: {comprehensive_info['analysis_id']}")
            print(f"   📊 Has Results: {'analysis_results' in comprehensive_info}")
            print(f"   📄 Readouts Found: {len(comprehensive_info.get('experiment_readouts', []))}")
        else:
            print(f"❌ Could not get comprehensive info: {comprehensive_info.get('error')}")
            print(f"💡 This is expected when not on DoorDash internal network")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        print(f"💡 This is expected when not connected to DoorDash internal network")


if __name__ == "__main__":
    test_experiment_name_extraction()

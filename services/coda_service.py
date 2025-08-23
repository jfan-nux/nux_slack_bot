from datetime import datetime
from typing import Dict, List, Optional, Any
from integrations.coda_client import CodaClient
from utils.logger import logger


class CodaService:
    """
    Service layer for Coda integration, handling business logic for scraping 
    and transforming Coda data for the NUX experiment tracking system.
    """
    
    def __init__(self, coda_client: CodaClient = None):
        self.client = coda_client or CodaClient()
        
        # Known experiment to project mappings (manual overrides)
        self.experiment_project_mappings = {
            "should_enable_percentage_eta_threshold_job": "FNY MS2",
            "cx_mobile_onboarding_preferences": "Onboarding in-line",
            "prominence_bottom_sheet": "Prominence re-test",
            "smart_app_banner_detection": "Smart app banner redirect"
        }
        
        # Mode template type mappings based on project characteristics
        self.project_mode_templates = {
            "FNY MS2": "funnel_dashpass",
            "Onboarding in-line": "app_downloads", 
            "Prominence re-test": "app_downloads",
            "Smart app banner redirect": "app_downloads",
            "App Clips": "app_clips"  # Future experiments
        }
    
    def scrape_q3_roadmap(self, coda_url: str) -> Dict[str, Any]:
        """
        Scrape the Q3 2025 Roadmap overview from Coda.
        
        Args:
            coda_url: Full Coda URL to the roadmap document
            
        Returns:
            Dictionary containing scraped roadmap data with metadata
        """
        logger.info("Starting Q3 2025 Roadmap scrape from Coda")
        
        try:
            # Scrape the "Q3 2025 Roadmap overview" table
            raw_data = self.client.scrape_table_by_url_and_name(
                coda_url, 
                "Q3 2025 Roadmap overview"
            )
            
            # Add scraping timestamp
            raw_data['scraped_at'] = datetime.now().isoformat()
            
            # Transform the data for our database schema
            transformed_projects = self.transform_roadmap_data(raw_data['rows'])
            
            result = {
                'success': True,
                'scraped_at': raw_data['scraped_at'],
                'source': {
                    'doc_name': raw_data['doc_info']['name'],
                    'table_name': raw_data['table_info']['name'],
                    'url': coda_url
                },
                'projects_found': len(transformed_projects),
                'projects': transformed_projects,
                'raw_data': raw_data  # Keep for debugging
            }
            
            logger.info(f"Successfully scraped {len(transformed_projects)} projects from Q3 roadmap")
            return result
            
        except Exception as e:
            logger.error(f"Failed to scrape Q3 roadmap: {e}")
            return {
                'success': False,
                'error': str(e),
                'scraped_at': datetime.now().isoformat()
            }
    
    def transform_roadmap_data(self, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw Coda data to match our project_experiment_states schema.
        
        Args:
            raw_rows: Raw rows from Coda API
            
        Returns:
            List of transformed project dictionaries
        """
        logger.info(f"Transforming {len(raw_rows)} raw rows from Coda")
        
        transformed_projects = []
        
        for row in raw_rows:
            try:
                # Extract project information (adjust field names based on actual Coda columns)
                project_name = self.get_cell_value(row, 'Project Name') or self.get_cell_value(row, 'project_name')
                if not project_name:
                    logger.warning(f"Skipping row without project name: {row}")
                    continue
                
                # Map experiment name if this project has a known experiment
                experiment_name = self.find_experiment_for_project(project_name)
                
                # Extract all the fields we need
                transformed_project = {
                    'experiment_name': experiment_name,
                    'project_name': project_name,
                    'project_description': self.get_cell_value(row, 'Details') or self.get_cell_value(row, 'description'),
                    'project_status': self.get_cell_value(row, 'Project Status') or self.get_cell_value(row, 'status'),
                    'owner': self.get_cell_value(row, 'Owner') or self.get_cell_value(row, 'owner'),
                    'dri': self.get_cell_value(row, 'DRI') or self.get_cell_value(row, 'dri'),
                    'planned_for': self.get_cell_value(row, 'Planned for') or self.get_cell_value(row, 'planned_for'),
                    'end_date': self.parse_date(self.get_cell_value(row, 'End Date') or self.get_cell_value(row, 'end_date')),
                    
                    # Resource links
                    'brief_link': self.extract_link(row, 'Brief'),
                    'figma_link': self.extract_link(row, 'Figma'),
                    'rfc_link': self.extract_link(row, 'RFCs') or self.extract_link(row, 'RFC'),
                    'jira_link': self.extract_link(row, 'JIRA') or self.extract_link(row, 'Jira'),
                    'dv_link': self.extract_link(row, 'DV'),
                    'curie_link': self.extract_link(row, 'Curie'),
                    'mode_link': self.extract_link(row, 'Mode'),
                    'readout_link': self.extract_link(row, 'Readout'),
                    
                    # Determine mode template type
                    'mode_template_type': self.determine_mode_template_type(project_name),
                    
                    # Calculate days since start (if we have launch data)
                    'day': self.calculate_days_since_start(row),
                    
                    # Metadata
                    'is_active': self.is_project_active(self.get_cell_value(row, 'Project Status')),
                    'coda_row_id': row.get('row_id'),
                    'last_updated': datetime.now().isoformat()
                }
                
                transformed_projects.append(transformed_project)
                logger.debug(f"Transformed project: {project_name} -> {experiment_name}")
                
            except Exception as e:
                logger.error(f"Failed to transform row {row.get('row_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_projects)} projects")
        return transformed_projects
    
    def get_cell_value(self, row: Dict[str, Any], key: str) -> Optional[str]:
        """Get value from row, trying multiple possible key variations."""
        # Try exact match first
        if key in row:
            return row[key]
        
        # Try case variations
        for row_key, row_value in row.items():
            if row_key.lower() == key.lower():
                return row_value
        
        return None
    
    def extract_link(self, row: Dict[str, Any], link_type: str) -> Optional[str]:
        """Extract link from a cell that might contain link objects."""
        cell_value = self.get_cell_value(row, link_type)
        
        if not cell_value:
            return None
            
        # If it's already a string URL, return it
        if isinstance(cell_value, str) and cell_value.startswith('http'):
            return cell_value
        
        # Handle Coda link objects
        if isinstance(cell_value, dict):
            return cell_value.get('url') or cell_value.get('href')
        
        return str(cell_value) if cell_value else None
    
    def parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse date string to ISO format."""
        if not date_str:
            return None
            
        try:
            # Handle common date formats
            from dateutil.parser import parse
            parsed_date = parse(str(date_str))
            return parsed_date.date().isoformat()
        except:
            logger.warning(f"Could not parse date: {date_str}")
            return None
    
    def find_experiment_for_project(self, project_name: str) -> Optional[str]:
        """Find the experiment name associated with a project."""
        # Check manual mappings first
        for experiment_name, mapped_project in self.experiment_project_mappings.items():
            if mapped_project.lower() in project_name.lower():
                return experiment_name
        
        # Future: Add fuzzy matching for %growth_nux% experiments
        
        return None
    
    def determine_mode_template_type(self, project_name: str) -> str:
        """Determine which Mode template to use based on project characteristics."""
        project_name_lower = project_name.lower()
        
        # Check for specific patterns
        if 'app clip' in project_name_lower:
            return 'app_clips'
        elif any(keyword in project_name_lower for keyword in ['funnel', 'dashpass', 'eta']):
            return 'funnel_dashpass'
        else:
            return 'app_downloads'  # Default template
    
    def calculate_days_since_start(self, row: Dict[str, Any]) -> Optional[int]:
        """Calculate days since experiment/project started."""
        start_date_str = (self.get_cell_value(row, 'Launch Day') or 
                         self.get_cell_value(row, 'Kick Off Date'))
        
        if not start_date_str:
            return None
            
        try:
            from dateutil.parser import parse
            start_date = parse(str(start_date_str)).date()
            days_since = (datetime.now().date() - start_date).days
            return max(0, days_since)
        except:
            return None
    
    def is_project_active(self, status: Optional[str]) -> bool:
        """Determine if project is currently active."""
        if not status:
            return False
            
        active_statuses = [
            'in experiment', 
            'testing early', 
            'in progress', 
            'active'
        ]
        
        return status.lower() in active_statuses
    
    def get_active_experiments(self, roadmap_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract only active experiments from roadmap data."""
        if not roadmap_data.get('success'):
            return []
        
        active_projects = []
        for project in roadmap_data.get('projects', []):
            if project.get('is_active') and project.get('experiment_name'):
                active_projects.append(project)
        
        logger.info(f"Found {len(active_projects)} active experiments")
        return active_projects


# Example usage and testing
if __name__ == "__main__":
    service = CodaService()
    
    # Test scraping the Q3 roadmap
    coda_url = "https://coda.io/d/nux-product_dn6rnftKCGZ/Q3-2025_su5L-0_F#Q3-2025-Roadmap-overview_tuWR35uZ/r621&columnId=c-QNPzuvsx3k"
    
    try:
        result = service.scrape_q3_roadmap(coda_url)
        
        if result['success']:
            print(f"✅ Successfully scraped {result['projects_found']} projects")
            
            active_experiments = service.get_active_experiments(result)
            print(f"🧪 Found {len(active_experiments)} active experiments:")
            
            for exp in active_experiments:
                print(f"  - {exp['experiment_name']} ({exp['project_name']})")
                
        else:
            print(f"❌ Scraping failed: {result['error']}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

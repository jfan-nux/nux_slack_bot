import requests
import re
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, parse_qs
from config.api_keys import CODA_API_KEY, CODA_BASE_URL
from utils.logger import logger


class CodaClient:
    """
    Coda API client for interacting with Coda documents and tables.
    Based on Coda API v1 documentation: https://coda.io/developers/apis/v1
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or CODA_API_KEY
        print(f"Coda API Key: {self.api_key}")
        self.base_url = CODA_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        })
        
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make a request to the Coda API with error handling."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            
            if response.status_code == 401:
                raise Exception("Coda API token is invalid or expired")
            elif response.status_code == 429:
                raise Exception("Coda API rate limit exceeded")
            elif response.status_code == 404:
                raise Exception(f"Coda resource not found: {endpoint}")
            elif response.status_code >= 400:
                raise Exception(f"Coda API error {response.status_code}: {response.text}")
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Coda API request failed: {e}")
            raise Exception(f"Failed to connect to Coda API: {e}")
    
    def parse_coda_url(self, coda_url: str) -> Dict[str, str]:
        """
        Return doc_id and optional view_id (from URL fragment).
        """
        try:
            m = re.search(r"/d/([^/]+)", coda_url)
            if not m:
                raise ValueError("Could not extract doc slug+id segment after /d/")
            slug_and_id = m.group(1)  # e.g., "nux-product_dn6rnftKCGZ"
            # doc_id is the token after the last underscore
            parts = slug_and_id.split("_")
            if len(parts) < 2:
                raise ValueError("URL is missing the _<docId> suffix")
            doc_id = parts[-1]  # "dn6rnftKCGZ"

            # Optional: capture the first fragment token (often a view/section id)
            view_id = None
            if "#" in coda_url:
                frag = coda_url.split("#", 1)[1]
                view_id = frag.split("/", 1)[0].split("&", 1)[0] or None

            return {"doc_id": doc_id, "view_id": view_id, "full_url": coda_url}
        except Exception as e:
            logger.error(f"Failed to parse Coda URL {coda_url}: {e}")
            raise ValueError(f"Invalid Coda URL format: {e}")

    
    def get_doc_info(self, doc_id: str) -> Dict[str, Any]:
        """Get basic information about a Coda document."""
        logger.info(f"Fetching doc info for {doc_id}")
        return self._make_request('GET', f'/docs/{doc_id}')
    
    def list_tables(self, doc_id: str) -> List[Dict[str, Any]]:
        """List all tables in a Coda document."""
        logger.info(f"Listing tables for doc {doc_id}")
        response = self._make_request('GET', f'/docs/{doc_id}/tables')
        return response.get('items', [])
    
    def get_table_info(self, doc_id: str, table_id: str) -> Dict[str, Any]:
        """Get information about a specific table."""
        logger.info(f"Fetching table info for {doc_id}/{table_id}")
        return self._make_request('GET', f'/docs/{doc_id}/tables/{table_id}')
    
    def list_columns(self, doc_id: str, table_id: str) -> List[Dict[str, Any]]:
        """List all columns in a table."""
        logger.info(f"Listing columns for {doc_id}/{table_id}")
        response = self._make_request('GET', f'/docs/{doc_id}/tables/{table_id}/columns')
        return response.get('items', [])
    
    def get_table_rows(self, doc_id: str, table_id: str, limit: int = 100, 
                      page_token: str = None) -> Dict[str, Any]:
        """
        Get rows from a Coda table or view.
        
        Args:
            doc_id: The document ID
            table_id: The table ID or view ID
            limit: Maximum number of rows to return (default 100)
            page_token: Token for pagination
        
        Returns:
            Dictionary containing items, nextPageToken, and nextPageLink
        """
        logger.info(f"Fetching rows from {doc_id}/{table_id} (limit: {limit})")
        
        params = {
            'limit': limit,
            'useColumnNames': 'true',  # Get readable column names instead of IDs
            'valueFormat': 'simple'    # Get clean values without markup
        }
        if page_token:
            params['pageToken'] = page_token
            
        response = self._make_request('GET', f'/docs/{doc_id}/tables/{table_id}/rows', 
                                    params=params)
        return response
    
    def get_all_table_rows(self, doc_id: str, table_id: str) -> List[Dict[str, Any]]:
        """
        Get all rows from a table, handling pagination automatically.
        """
        logger.info(f"Fetching all rows from {doc_id}/{table_id}")
        
        all_rows = []
        page_token = None
        
        while True:
            response = self.get_table_rows(doc_id, table_id, limit=100, 
                                         page_token=page_token)
            
            rows = response.get('items', [])
            all_rows.extend(rows)
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
                
            logger.info(f"Fetched {len(all_rows)} rows so far, continuing...")
        
        logger.info(f"Fetched {len(all_rows)} total rows from {table_id}")
        return all_rows
    
    def find_table_by_name(self, doc_id: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Find a table by its name in the document."""
        logger.info(f"Looking for table '{table_name}' in doc {doc_id}")
        
        tables = self.list_tables(doc_id)
        for table in tables:
            if table.get('name', '').lower() == table_name.lower():
                logger.info(f"Found table '{table_name}' with ID {table.get('id')}")
                return table
                
        logger.warning(f"Table '{table_name}' not found in doc {doc_id}")
        return None
    
    def scrape_table_by_url_and_name(self, coda_url: str, section_name: str) -> Dict[str, Any]:
        """
        Scrape a specific table or view from a Coda document.
        
        Args:
            coda_url: Full Coda URL 
            section_name: Name of the table/section to scrape
        
        Returns:
            Dictionary containing table data and metadata
        """
        logger.info(f"Scraping Coda section '{section_name}' from {coda_url}")
        
        try:
            # Parse the URL to get doc_id and optional view_id
            url_parts = self.parse_coda_url(coda_url)
            doc_id = url_parts['doc_id']
            view_id = url_parts['view_id']
            
            # Try to get doc info (may fail for scoped tokens)
            doc_info = None
            try:
                doc_info = self.get_doc_info(doc_id)
                logger.info(f"Connected to doc: {doc_info.get('name')}")
            except Exception as e:
                logger.info(f"Cannot access doc info (scoped token): {e}")
                doc_info = {'id': doc_id, 'name': 'Unknown (Scoped Access)'}
            
            # Try the fragment as a view id first (if present)
            table_id = None
            if view_id:
                try:
                    # Test if the view_id is a valid table/view
                    _ = self.get_table_info(doc_id, view_id)
                    table_id = view_id
                    logger.info(f"Using view id from fragment as tableId: {view_id}")
                except Exception:
                    logger.info("Fragment isn't a valid view id; will look up by name instead.")
            
            # Fall back to finding table by name if view_id didn't work
            if not table_id:
                table = self.find_table_by_name(doc_id, section_name)
                if not table:
                    raise ValueError(f"Table/View '{section_name}' not found in document")
                table_id = table['id']
                logger.info(f"Found table '{section_name}' with ID: {table_id}")
            
            # Get table columns for context (still useful for metadata)
            columns = self.list_columns(doc_id, table_id)
            column_info = {col['id']: col.get('name', col['id']) for col in columns}
            
            # Get all table rows (now with useColumnNames=true, values will have readable keys)
            rows = self.get_all_table_rows(doc_id, table_id)
            
            # Transform the data - much simpler now with useColumnNames=true
            transformed_rows = []
            for row in rows:
                row_data = {'row_id': row.get('id')}
                # With useColumnNames=true, row values already have readable column names
                row_data.update(row.get('values', {}))
                transformed_rows.append(row_data)
            
            result = {
                'doc_info': {
                    'id': doc_id,
                    'name': doc_info.get('name'),
                    'url': coda_url
                },
                'table_info': {
                    'id': table_id,
                    'name': section_name,
                    'row_count': len(transformed_rows)
                },
                'columns': column_info,
                'rows': transformed_rows,
                'scraped_at': None  # Will be set by the service
            }
            
            logger.info(f"Successfully scraped {len(transformed_rows)} rows from '{section_name}'")
            return result
            
        except Exception as e:
            logger.error(f"Failed to scrape Coda table: {e}")
            raise


# Example usage and testing
if __name__ == "__main__":
    # Test the Coda client
    client = CodaClient()
    
    # Example Coda URL from your project
    test_url = "https://coda.io/d/nux-product_dn6rnftKCGZ/Q3-2025_su5L-0_F#Q3-2025-Roadmap-overview_tuWR35uZ/r621&columnId=c-QNPzuvsx3k"
    section_name = "Q3 2025 Roadmap overview"
    
    try:
        result = client.scrape_table_by_url_and_name(test_url, section_name)
        print(f"✅ Successfully scraped {result['table_info']['row_count']} rows")
        print(f"📊 Columns: {list(result['columns'].values())}")
        
        # Print first row as example
        if result['rows']:
            print(f"📝 Sample row: {result['rows'][0]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

#!/usr/bin/env python3
"""
Main entry point for the NUX Slack Bot experiment monitoring system.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.coda_service import CodaService
from config.api_keys import validate_required_env_vars
from utils.logger import logger


async def main():
    """Main entry point for the application."""
    logger.info("Starting NUX Slack Bot System")
    
    try:
        # Validate environment
        validate_required_env_vars()
        logger.info("✅ Environment variables validated")
        
        # Initialize services
        coda_service = CodaService()
        
        # Test Coda integration
        logger.info("Testing Coda integration...")
        coda_url = "https://coda.io/d/nux-product_dn6rnftKCGZ/Q3-2025_su5L-0_F#Q3-2025-Roadmap-overview_tuWR35uZ/r621&columnId=c-QNPzuvsx3k"
        
        result = coda_service.scrape_q3_roadmap(coda_url)
        
        if result['success']:
            logger.info(f"✅ Successfully scraped {result['projects_found']} projects")
            
            active_experiments = coda_service.get_active_experiments(result)
            logger.info(f"🧪 Found {len(active_experiments)} active experiments")
            
            for exp in active_experiments:
                logger.info(f"  - {exp['experiment_name']} ({exp['project_name']})")
        else:
            logger.error(f"❌ Coda scraping failed: {result['error']}")
            
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())


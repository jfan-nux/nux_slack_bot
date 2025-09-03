#!/usr/bin/env python3
"""
Script to build wheel package for nux_slack_bot.
Run this to create a .whl file for Databricks installation.
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Build wheel package for distribution."""
    print("🔨 Building nux_slack_bot wheel package...")
    
    # Ensure we're in the right directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    try:
        # Install build dependencies
        print("📦 Installing build dependencies...")
        subprocess.run([sys.executable, "-m", "pip", "install", "build", "wheel"], 
                      check=True)
        
        # Clean previous builds
        print("🧹 Cleaning previous builds...")
        import shutil
        for dir_name in ["build", "dist", "*.egg-info"]:
            for path in script_dir.glob(dir_name):
                if path.is_dir():
                    shutil.rmtree(path)
                    print(f"  Removed {path}")
        
        # Build the package
        print("🏗️  Building package...")
        result = subprocess.run([sys.executable, "-m", "build"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Build successful!")
            
            # List created files
            dist_dir = script_dir / "dist"
            if dist_dir.exists():
                print("\n📁 Created files:")
                for file in dist_dir.iterdir():
                    print(f"  📄 {file.name}")
                
                # Find wheel file
                wheel_files = list(dist_dir.glob("*.whl"))
                if wheel_files:
                    wheel_file = wheel_files[0]
                    print(f"\n🎯 Main wheel file: {wheel_file.name}")
                    print(f"📍 Full path: {wheel_file}")
                    
                    print("\n🚀 Installation commands:")
                    print("   Local install:")
                    print(f"     pip install {wheel_file}")
                    print("   Databricks install:")
                    print(f"     %pip install {wheel_file}")
                    print("   Or from GitHub:")
                    print("     %pip install git+https://github.com/jfan-nux/nux_slack_bot.git")
            
        else:
            print("❌ Build failed!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return 1
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Error during build: {e}")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Complete E-commerce Product URL Pipeline
Orchestrates the complete flow:
1. Runs ecommerce_stealth_crawler_fixed.py to get search results
2. Runs markdown_product_url_extractor.py to extract product URLs intelligently
3. Manages API limits and timing effectively
"""

import asyncio
import subprocess
import time
import os
import json
import sys
from datetime import datetime
from typing import List, Dict, Any

class EcommercePipeline:
    def __init__(self):
        """Initialize the complete e-commerce pipeline."""
        self.pipeline_start_time = time.time()
        
        # File tracking
        self.files_created = []
        
        # API management for markdown extractor
        self.chunk_size = 3000  # Smaller chunks to avoid API limits
        self.delay_between_chunks = 5  # 5 seconds between chunks
        self.max_chunks_per_batch = 10  # Process max 10 chunks, then longer break
        self.batch_break_time = 30  # 30 seconds break between batches
        
        # Pipeline steps configuration
        self.pipeline_steps = {
            1: {
                "name": "E-commerce Search Crawling",
                "script": "ecommerce_stealth_crawler_fixed.py",
                "description": "Search and crawl e-commerce site (creates rakuten.md)",
                "output_file": "rakuten.md",
                "required_input": None
            },
            2: {
                "name": "AI Product URL Extraction",
                "script": "markdown_product_url_extractor.py", 
                "description": "Extract product URLs from markdown using AI",
                "output_file": "rakuten_product_urls_from_markdown.json",
                "required_input": "rakuten.md"
            },
            3: {
                "name": "Bulk Product Scraping",
                "script": "rakuten_bulk_product_scraper.py",
                "description": "Scrape product details from URLs",
                "output_file": "rakuten.json",
                "required_input": "rakuten_product_urls_from_markdown.json"
            },
            4: {
                "name": "Gemini Product Processing",
                "script": "rakuten_gemini_processor.py",
                "description": "Extract structured product data using Gemini AI",
                "output_file": "rakuten_final.json",
                "required_input": "rakuten.json"
            },
            5: {
                "name": "CSV Conversion",
                "script": "rakuten_csv_converter.py",
                "description": "Convert structured data to CSV format",
                "output_file": "rakuten.csv",
                "required_input": "rakuten_final.json"
            }
        }
        
        print("🚀 Complete E-commerce Pipeline Initialized")
        print("=" * 60)

    def run_script(self, script_name: str, description: str) -> bool:
        """Run a Python script and return success status."""
        try:
            print(f"\n🔄 Step: {description}")
            print(f"📄 Running: {script_name}")
            print("-" * 40)
            
            # Start timing
            step_start = time.time()
            
            # Run the script with current Python environment
            result = subprocess.run([
                sys.executable, script_name
            ], capture_output=False, text=True, cwd=os.getcwd())
            
            step_time = time.time() - step_start
            
            if result.returncode == 0:
                print(f"✅ {description} completed successfully")
                print(f"⏱️ Time taken: {step_time:.1f} seconds")
                return True
            else:
                print(f"❌ {description} failed with return code: {result.returncode}")
                return False
                
        except Exception as e:
            print(f"❌ Error running {script_name}: {str(e)}")
            return False

    def check_file_exists(self, filename: str, description: str) -> bool:
        """Check if a required file exists."""
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            print(f"✅ {description} exists: {filename} ({file_size:,} bytes)")
            return True
        else:
            print(f"❌ {description} missing: {filename}")
            return False

    def get_file_stats(self, filename: str) -> Dict[str, Any]:
        """Get statistics about a file."""
        try:
            if not os.path.exists(filename):
                return {"exists": False}
            
            stats = {
                "exists": True,
                "size_bytes": os.path.getsize(filename),
                "modified_time": datetime.fromtimestamp(os.path.getmtime(filename)).isoformat()
            }
            
            # Additional stats for specific file types
            if filename.endswith('.md'):
                with open(filename, 'r', encoding='utf-8') as f:
                    content = f.read()
                    stats["content_length"] = len(content)
                    stats["line_count"] = len(content.split('\n'))
            
            elif filename.endswith('.json'):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        if "product_urls" in data:
                            stats["url_count"] = len(data["product_urls"])
                        elif "products" in data:
                            stats["product_count"] = len(data["products"])
            
            return stats
            
        except Exception as e:
            return {"exists": True, "error": str(e)}

    def check_input_requirements(self, step_num: int) -> bool:
        """Check if required input files exist for a given step."""
        step = self.pipeline_steps.get(step_num)
        if not step:
            return False
            
        required_input = step.get("required_input")
        if not required_input:
            return True  # No input required
            
        if os.path.exists(required_input):
            file_size = os.path.getsize(required_input)
            print(f"✅ Required input exists: {required_input} ({file_size:,} bytes)")
            return True
        else:
            print(f"❌ Required input missing: {required_input}")
            return False

    def display_pipeline_options(self):
        """Display available pipeline steps and options."""
        print(f"\n📋 AVAILABLE PIPELINE STEPS:")
        print("-" * 60)
        for step_num, step_info in self.pipeline_steps.items():
            status = "✅" if not step_info["required_input"] or os.path.exists(step_info["required_input"]) else "❌"
            print(f"  {step_num}. {status} {step_info['name']}")
            print(f"     → {step_info['description']}")
            if step_info["required_input"]:
                print(f"     📄 Requires: {step_info['required_input']}")
            print(f"     💾 Outputs: {step_info['output_file']}")
            print()

    def run_pipeline_from_step(self, start_step: int = 1, end_step: int = None):
        """Run pipeline starting from a specific step."""
        if end_step is None:
            end_step = len(self.pipeline_steps)
            
        print(f"🎯 Running pipeline from step {start_step} to {end_step}")
        
        # Validate step range
        if start_step < 1 or start_step > len(self.pipeline_steps):
            print(f"❌ Invalid start step: {start_step}")
            return False
            
        if end_step < start_step or end_step > len(self.pipeline_steps):
            print(f"❌ Invalid end step: {end_step}")
            return False

        success_steps = 0
        
        for step_num in range(start_step, end_step + 1):
            step_info = self.pipeline_steps[step_num]
            
            print(f"\n{'='*60}")
            print(f"STEP {step_num}/{len(self.pipeline_steps)}: {step_info['name'].upper()}")
            print(f"{'='*60}")
            
            # Check input requirements
            if not self.check_input_requirements(step_num):
                print(f"❌ Step {step_num} cannot proceed - missing required input")
                return False
            
            # Run the step
            if self.run_script(step_info['script'], step_info['description']):
                success_steps += 1
                
                # Check if output was created
                if self.check_file_exists(step_info['output_file'], f"Step {step_num} output"):
                    self.files_created.append(step_info['output_file'])
                else:
                    print(f"❌ Step {step_num} output not found - pipeline stopped")
                    return False
                    
                # Add delay between steps (except for the last step)
                if step_num < end_step:
                    print(f"\n⏳ Waiting 10 seconds before next step...")
                    time.sleep(10)
            else:
                print(f"❌ Step {step_num} failed - pipeline stopped")
                return False
        
        # Pipeline completion summary
        total_time = time.time() - self.pipeline_start_time
        
        print(f"\n{'='*60}")
        print(f"🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"{'='*60}")
        print(f"✅ Steps completed: {success_steps}/{end_step - start_step + 1}")
        print(f"⏱️ Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"📁 Files created: {len(self.files_created)}")
        
        for file in self.files_created:
            stats = self.get_file_stats(file)
            size_info = f" ({stats.get('size_bytes', 0):,} bytes)" if stats.get('exists') else " (missing)"
            print(f"   • {file}{size_info}")
        
        return True

    def update_markdown_extractor_settings(self):
        """Update markdown extractor with optimized API settings."""
        try:
            # Read the current file
            with open('markdown_product_url_extractor.py', 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Update settings for better API management
            updated_content = content.replace(
                'self.chunk_size = 4000  # Characters per chunk to process',
                f'self.chunk_size = {self.chunk_size}  # Optimized for API limits'
            ).replace(
                'self.delay_between_requests = 2  # Seconds between API calls',
                f'self.delay_between_requests = {self.delay_between_chunks}  # Optimized delay'
            )
            
            # Add batch processing logic
            if 'batch_break_time' not in updated_content:
                updated_content = updated_content.replace(
                    'self.delay_between_requests = 5  # Optimized delay',
                    f'self.delay_between_requests = {self.delay_between_chunks}  # Optimized delay\n        self.max_chunks_per_batch = {self.max_chunks_per_batch}  # Chunks per batch\n        self.batch_break_time = {self.batch_break_time}  # Break between batches'
                )
            
            # Write back the updated file
            with open('markdown_product_url_extractor.py', 'w', encoding='utf-8') as f:
                f.write(updated_content)
            
            print(f"⚙️ Updated markdown extractor settings:")
            print(f"   - Chunk size: {self.chunk_size} characters")
            print(f"   - Delay between chunks: {self.delay_between_chunks} seconds")
            print(f"   - Max chunks per batch: {self.max_chunks_per_batch}")
            print(f"   - Batch break time: {self.batch_break_time} seconds")
            
        except Exception as e:
            print(f"⚠️ Could not update markdown extractor settings: {str(e)}")

def main():
    """Main function to run the complete pipeline."""
    print("🔄 Complete E-commerce Pipeline")
    print("=" * 60)
    
    # Initialize pipeline
    pipeline = EcommercePipeline()
    
    # Check if all scripts exist
    all_scripts = [
        'ecommerce_stealth_crawler_fixed.py',
        'markdown_product_url_extractor.py',
        'rakuten_bulk_product_scraper.py',
        'rakuten_gemini_processor.py',
        'rakuten_csv_converter.py'
    ]
    
    missing_scripts = []
    available_scripts = []
    for script in all_scripts:
        if os.path.exists(script):
            available_scripts.append(script)
        else:
            missing_scripts.append(script)
    
    print(f"✅ Available scripts: {len(available_scripts)}/{len(all_scripts)}")
    if missing_scripts:
        print(f"⚠️ Missing scripts: {', '.join(missing_scripts)}")
    
    # Display pipeline options
    pipeline.display_pipeline_options()
    
    # Ask user what they want to do
    try:
        print(f"\n🚀 PIPELINE OPTIONS:")
        print(f"   1. Run complete pipeline (steps 1-5)")
        print(f"   2. Run from specific step")
        print(f"   3. Run single step only")
        print(f"   4. Exit")
        
        choice = input(f"\nSelect option (1-4): ").strip()
        
        if choice == "1":
            # Run complete pipeline
            print(f"\n🎯 Running complete 5-step pipeline...")
            success = pipeline.run_pipeline_from_step(1, 5)
            
        elif choice == "2":
            # Run from specific step
            start = input(f"\nEnter start step (1-5): ").strip()
            end = input(f"Enter end step (1-5, or press Enter for end): ").strip()
            
            try:
                start_step = int(start)
                end_step = int(end) if end else 5
                success = pipeline.run_pipeline_from_step(start_step, end_step)
            except ValueError:
                print(f"❌ Invalid step numbers")
                return
                
        elif choice == "3":
            # Run single step
            step = input(f"\nEnter step number (1-5): ").strip()
            
            try:
                step_num = int(step)
                success = pipeline.run_pipeline_from_step(step_num, step_num)
            except ValueError:
                print(f"❌ Invalid step number")
                return
                
        elif choice == "4":
            print(f"👋 Goodbye!")
            return
            
        else:
            print(f"❌ Invalid choice")
            return
        
        # Show results
        if success:
            print(f"\n🎉 Pipeline completed successfully!")
            
            # Show final file statistics
            final_files = ["rakuten.md", "rakuten_product_urls_from_markdown.json", 
                          "rakuten.json", "rakuten_final.json", "rakuten.csv"]
            
            print(f"\n📊 FINAL FILE STATUS:")
            for file in final_files:
                if os.path.exists(file):
                    size = os.path.getsize(file)
                    print(f"   ✅ {file} ({size:,} bytes)")
                else:
                    print(f"   ❌ {file} (not created)")
                    
        else:
            print(f"\n❌ Pipeline failed or was interrupted!")
        
        input(f"\nPress Enter to exit...")
            
    except (EOFError, KeyboardInterrupt):
        print(f"\n❌ Pipeline interrupted by user")

if __name__ == "__main__":
    main()

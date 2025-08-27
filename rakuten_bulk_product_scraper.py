#!/usr/bin/env python3
"""
Bulk Rakuten Product Scraper
Reads URLs from rakuten_product_urls_from_markdown.json, validates them, and scrapes product data
"""

import asyncio
import json
import re
import os
from typing import List, Dict, Set
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

def validate_product_url(url: str) -> bool:
    """
    Validate if a URL is a proper Rakuten product URL
    
    Args:
        url (str): URL to validate
        
    Returns:
        bool: True if valid product URL, False otherwise
    """
    
    # Basic validation patterns
    invalid_patterns = [
        
    ]
    
    # Check against invalid patterns
    for pattern in invalid_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return False
    
    # Valid product URLs should have meaningful product IDs
    # Extract the product ID part
    match = re.search(r'item\.rakuten\.co\.jp/[^/]+/([^/?]+)', url)
    if match:
        product_id = match.group(1)
        
        # Valid product IDs are usually:
        # - Long numeric codes (8+ digits)
        # - Alphanumeric codes with meaningful length (5+ chars)
        # - Not generic words
        
        if len(product_id) >= 5:
            # Check if it's mostly numeric (good sign)
            if re.match(r'^\d{8,}', product_id):
                return True
            # Check if it's alphanumeric with good length
            if re.match(r'^[a-zA-Z0-9\-_]{5,}$', product_id) and not re.match(r'^(item|product|aa|zakka)\d*$', product_id, re.IGNORECASE):
                return True
    
    return False

def load_urls_from_file(file_path: str = 'rakuten_product_urls_from_markdown.json') -> List[str]:
    """
    Load URLs from JSON file and validate them
    
    Args:
        file_path (str): Path to the URLs JSON file
        
    Returns:
        List[str]: List of valid product URLs
    """
    
    if not os.path.exists(file_path):
        print(f"❌ File {file_path} not found!")
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract URLs from JSON structure
        all_urls = data.get('product_urls', [])
        
        print(f"📖 Loaded {len(all_urls)} URLs from {file_path}")
        print(f"🌐 Site: {data.get('extraction_metadata', {}).get('site_name', 'unknown')}")
        print(f"🤖 Extraction Method: {data.get('extraction_metadata', {}).get('extraction_method', 'unknown')}")
        
        # Only filter out obvious review URLs, keep everything else
        valid_urls = []
        filtered_urls = []
        
        for url in all_urls:
            # Only exclude review URLs since they're not product pages
            if 'review.rakuten.co.jp' in url:
                filtered_urls.append(url)
            else:
                valid_urls.append(url)
        
        print(f"✅ Product URLs to scrape: {len(valid_urls)}")
        print(f"🚫 Filtered review URLs: {len(filtered_urls)}")
        
        if filtered_urls:
            print(f"\n🚫 Sample filtered review URLs:")
            for i, url in enumerate(filtered_urls[:3]):
                print(f"   {i+1}. {url}")
            if len(filtered_urls) > 3:
                print(f"   ... and {len(filtered_urls) - 3} more")
        
        return valid_urls
        
    except Exception as e:
        print(f"❌ Error reading URLs file: {e}")
        return []

async def scrape_product_url(crawler, url: str, index: int, total: int) -> Dict:
    """
    Scrape a single product URL and return structured data with clean markdown (no links)
    
    Args:
        crawler: AsyncWebCrawler instance
        url (str): URL to scrape
        index (int): Current index
        total (int): Total number of URLs
        
    Returns:
        Dict: Scraped product data
    """
    
    print(f"🔗 [{index+1}/{total}] Scraping: {url}")
    
    try:
        # Configure the markdown generator EXACTLY like page.py - remove links only
        md_generator = DefaultMarkdownGenerator(
            content_source="raw_html",           # Use raw HTML - preserves ALL content!
            options={
                "ignore_links": True,           # ONLY remove links - this is our goal
                "ignore_images": False,         # Keep product images 
                "escape_html": True,           # Clean HTML entities
                "body_width": 0,               # No line wrapping
                "skip_internal_links": True,   # Skip anchors
                "include_sup_sub": True,       # Handle superscript/subscript
                "mark_code": True,             # Preserve code formatting
                "unicode_snob": True,          # Better Unicode handling
                "decode_errors": "ignore"      # Ignore encoding errors
            }
        )
        
        # Configure crawler with ADVANCED Crawl4AI features like page.py (optimized for Gemini processing)
        config = CrawlerRunConfig(
            markdown_generator=md_generator,
            
            # CONTENT PROCESSING (Optimized for Gemini)
            word_count_threshold=0,              # No word filtering - let Gemini decide
            only_text=False,                     # Keep structured HTML for Gemini context
            keep_data_attributes=True,           # Preserve product data attributes
            remove_forms=True,                   # Remove search/login forms
            excluded_tags=['script', 'style', 'noscript'],  # Minimal filtering
            
            # ENHANCED MEDIA HANDLING for Product Images
            wait_for_images=True,                # Wait for product images to load
            image_score_threshold=3,             # Filter low-quality images
            image_description_min_word_threshold=5,  # Better alt text filtering
            exclude_external_images=False,      # Keep all product images (even CDN)
            
            # LAZY LOADING & SCROLLING OPTIMIZATION
            scan_full_page=True,                 # Scroll entire page for lazy content
            scroll_delay=0.5,                    # Delay between scroll steps
            
            # SMART WAITING (instead of fixed delays)
            wait_for="js:() => document.querySelector('[data-asin], .product, #productTitle, .product-title, [class*=\"price\"], [id*=\"price\"]') !== null || document.querySelectorAll('img').length > 5",
            
            # ANTI-DETECTION (Enhanced)
            simulate_user=True,                  # Human-like interactions  
            override_navigator=True,             # Handle navigator detection
            magic=True,                          # Smart content detection
            remove_overlay_elements=True,        # Remove popups that block content
            
            # CACHE & PERFORMANCE
            cache_mode=CacheMode.BYPASS,         # Always get fresh product data
            page_timeout=60000,                  # 60 second timeout
            delay_before_return_html=8.0,        # Final wait before capture
            
            # ADVANCED JAVASCRIPT HANDLING (Using documentation best practices)
            js_code=[
                # Step 1: Initial page load wait
                "await new Promise(resolve => setTimeout(resolve, 2000));",
                
                # Step 2: Progressive scrolling to trigger lazy loading  
                "window.scrollTo(0, document.body.scrollHeight/4);",
                "await new Promise(resolve => setTimeout(resolve, 1500));",
                
                # Step 3: More scrolling for complete content
                "window.scrollTo(0, document.body.scrollHeight/2);", 
                "await new Promise(resolve => setTimeout(resolve, 1500));",
                "window.scrollTo(0, document.body.scrollHeight * 0.75);",
                "await new Promise(resolve => setTimeout(resolve, 1500));",
                
                # Step 4: Final scroll and wait
                "window.scrollTo(0, document.body.scrollHeight);",
                "await new Promise(resolve => setTimeout(resolve, 2000));",
            ]
        )
        
        result = await crawler.arun(url, config=config)
        
        if result.success:
            # Extract basic product info from URL
            url_match = re.search(r'item\.rakuten\.co\.jp/([^/]+)/([^/?]+)', url)
            shop_name = url_match.group(1) if url_match else "unknown"
            product_id = url_match.group(2) if url_match else "unknown"
            
            # Get the raw markdown with links removed (like page.py)
            if result.markdown and hasattr(result.markdown, 'raw_markdown'):
                clean_markdown = result.markdown.raw_markdown
            elif result.markdown:
                clean_markdown = str(result.markdown)
            else:
                clean_markdown = ""
            
            # Post-process to ensure ALL links are completely removed (EXACTLY like page.py)
            if clean_markdown:
                # Remove markdown links [text](url) - replace with just the text
                final_markdown = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', clean_markdown)
                
                # Remove any remaining markdown links with empty URLs []()
                final_markdown = re.sub(r'\[([^\]]*)\]\(\)', r'\1', final_markdown)
                
                # Remove any remaining standalone URLs (but preserve header URLs if any)
                lines = final_markdown.split('\n')
                cleaned_lines = []
                for i, line in enumerate(lines):
                    # Skip URL removal for potential header lines
                    if i < 10 and ('**Source URL:**' in line or '**Processed:**' in line):
                        cleaned_lines.append(line)
                    else:
                        # Remove URLs from content lines
                        line = re.sub(r'https?://[^\s\)]+', '', line)
                        cleaned_lines.append(line)
                final_markdown = '\n'.join(cleaned_lines)
                
                # Clean up any double spaces or empty lines created by link removal
                final_markdown = re.sub(r'\n\s*\n\s*\n', '\n\n', final_markdown)
                final_markdown = re.sub(r'  +', ' ', final_markdown)
            else:
                final_markdown = ""
            
            product_data = {
                "url": url,
                "shop_name": shop_name,
                "product_id": product_id,
                "markdown_content": final_markdown,  # Now clean markdown without links!
                "content_length": len(final_markdown),
                "scrape_success": True,
                "scrape_timestamp": result.session_id,
                "error_message": None
            }
            
            print(f"   ✅ Success - {len(final_markdown):,} characters (links removed)")
            return product_data
            
        else:
            print(f"   ❌ Failed: {result.error_message}")
            return {
                "url": url,
                "shop_name": "unknown",
                "product_id": "unknown", 
                "markdown_content": "",
                "content_length": 0,
                "scrape_success": False,
                "scrape_timestamp": None,
                "error_message": result.error_message
            }
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return {
            "url": url,
            "shop_name": "unknown",
            "product_id": "unknown",
            "markdown_content": "",
            "content_length": 0,
            "scrape_success": False,
            "scrape_timestamp": None,
            "error_message": str(e)
        }

async def bulk_scrape_products(urls: List[str], batch_size: int = 5) -> List[Dict]:
    """
    Scrape multiple product URLs in batches with advanced configuration like page.py
    
    Args:
        urls (List[str]): List of URLs to scrape
        batch_size (int): Number of concurrent requests
        
    Returns:
        List[Dict]: List of scraped product data
    """
    
    print(f"🚀 Starting bulk scrape of {len(urls)} URLs (batch size: {batch_size})")
    print("🔧 Using advanced configuration with link removal like page.py")
    print("=" * 70)
    
    all_results = []
    
    # Configure browser like page.py for optimal e-commerce scraping
    browser_config = BrowserConfig(
        headless=True,
        viewport_width=1920,             # Wide viewport for full product layouts
        viewport_height=1080,            # Tall viewport for complete product info
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        java_script_enabled=True,        # Essential for dynamic e-commerce content
        ignore_https_errors=True,        # Handle certificate issues
        text_mode=False,                 # Keep images for complete product data
    )
    
    async with AsyncWebCrawler(config=browser_config, verbose=False) as crawler:
        # Process URLs in batches to avoid overwhelming the server
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(urls) + batch_size - 1) // batch_size
            
            print(f"\n📦 Batch {batch_num}/{total_batches} - Processing {len(batch)} URLs")
            
            # Create tasks for this batch
            tasks = [
                scrape_product_url(crawler, url, i + j, len(urls))
                for j, url in enumerate(batch)
            ]
            
            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"   ❌ Batch exception: {result}")
                else:
                    all_results.append(result)
            
            # Small delay between batches
            if i + batch_size < len(urls):
                print(f"   ⏱️ Waiting 2 seconds before next batch...")
                await asyncio.sleep(2)
    
    return all_results

def save_results_to_json(results: List[Dict], output_file: str = 'rakuten.json') -> None:
    """
    Save scraped results to JSON file
    
    Args:
        results (List[Dict]): List of scraped data
        output_file (str): Output JSON file name
    """
    
    try:
        # Create summary statistics
        successful = [r for r in results if r.get('scrape_success', False)]
        failed = [r for r in results if not r.get('scrape_success', False)]
        
        total_content = sum(r.get('content_length', 0) for r in successful)
        
        # Prepare final data structure
        final_data = {
            "scrape_metadata": {
                "total_urls": len(results),
                "successful_scrapes": len(successful),
                "failed_scrapes": len(failed),
                "total_content_length": total_content,
                "success_rate": f"{(len(successful) / len(results) * 100):.1f}%" if results else "0%"
            },
            "products": results
        }
        
        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Results saved to {output_file}")
        print(f"📊 Success rate: {len(successful)}/{len(results)} ({(len(successful) / len(results) * 100):.1f}%)")
        print(f"📄 Total content: {total_content:,} characters")
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")

async def main():
    """Main function to orchestrate the bulk scraping process"""
    
    print("🛒 Rakuten Bulk Product Scraper")
    print("=" * 50)
    
    # Load and validate URLs
    urls = load_urls_from_file()
    
    if not urls:
        print("❌ No valid URLs to scrape!")
        return
    
    print(f"\n🎯 Will scrape {len(urls)} valid product URLs")
    
    print(f"\n🚀 Starting to scrape {len(urls)} URLs...")
    
    # Perform bulk scraping
    results = await bulk_scrape_products(urls, batch_size=3)  # Conservative batch size
    
    # Save results
    save_results_to_json(results)
    
    print(f"\n✅ Bulk scraping completed!")
    print(f"📄 Check rakuten.json for the scraped product data")

if __name__ == "__main__":
    asyncio.run(main())

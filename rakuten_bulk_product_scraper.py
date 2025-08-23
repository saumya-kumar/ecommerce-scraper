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
from crawl4ai import AsyncWebCrawler

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
    Scrape a single product URL and return structured data
    
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
        result = await crawler.arun(
            url=url,
            word_count_threshold=10,
            bypass_cache=True
        )
        
        if result.success:
            # Extract basic product info from URL
            url_match = re.search(r'item\.rakuten\.co\.jp/([^/]+)/([^/?]+)', url)
            shop_name = url_match.group(1) if url_match else "unknown"
            product_id = url_match.group(2) if url_match else "unknown"
            
            product_data = {
                "url": url,
                "shop_name": shop_name,
                "product_id": product_id,
                "markdown_content": result.markdown,
                "content_length": len(result.markdown),
                "scrape_success": True,
                "scrape_timestamp": result.session_id,
                "error_message": None
            }
            
            print(f"   ✅ Success - {len(result.markdown):,} characters")
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
    Scrape multiple product URLs in batches
    
    Args:
        urls (List[str]): List of URLs to scrape
        batch_size (int): Number of concurrent requests
        
    Returns:
        List[Dict]: List of scraped product data
    """
    
    print(f"🚀 Starting bulk scrape of {len(urls)} URLs (batch size: {batch_size})")
    print("=" * 70)
    
    all_results = []
    
    async with AsyncWebCrawler(verbose=False) as crawler:
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

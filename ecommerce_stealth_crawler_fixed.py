#!/usr/bin/env python3
"""
Generic Ecommerce Keyword Crawler
Searches and scrapes product data from multiple Japanese e-commerce sites
using keywords and saves markdown results for each website.
"""

import asyncio
import time
import logging
import json
import os
import re
from urllib.parse import quote
from crawl4ai import AsyncWebCrawler
from datetime import datetime
from typing import Dict, List
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GenericEcommerceCrawler:
    """Generic E-commerce Keyword Crawler for Japanese sites"""
    
    def __init__(self, keywords: List[str], rakuten: bool = True, amazon: bool = True, yahoo: bool = True, aupay: bool = True, cosme: bool = True):
        self.keywords = keywords
        self.results = {}
        
        # All available sites
        all_sites = {
            'rakuten': 'https://search.rakuten.co.jp/search/mall/{keyword}/',
            'amazon': 'https://www.amazon.co.jp/s?k={keyword}',
            'yahoo': 'https://shopping.yahoo.co.jp/search?p={keyword}',
            'aupay': 'https://wowma.jp/search?keyword={keyword}',
            'cosme': 'https://cosmeet.cosme.net/product/search?keyword={keyword}'
        }
        
        # Filter sites based on parameters
        self.sites = {}
        if rakuten:
            self.sites['rakuten'] = all_sites['rakuten']
        if amazon:
            self.sites['amazon'] = all_sites['amazon']
        if yahoo:
            self.sites['yahoo'] = all_sites['yahoo']
        if aupay:
            self.sites['aupay'] = all_sites['aupay']
        if cosme:
            self.sites['cosme'] = all_sites['cosme']
        
        # Ensure at least one site is selected
        if not self.sites:
            logger.warning("⚠️ No sites selected! Defaulting to Rakuten only.")
            self.sites = {'rakuten': all_sites['rakuten']}
        
    async def search_and_crawl_site(self, site_name: str, url_template: str, keyword: str) -> Dict:
        """Search and crawl a specific e-commerce site for a keyword"""
        try:
            # Encode keyword for URL
            encoded_keyword = quote(keyword)
            search_url = url_template.format(keyword=encoded_keyword)
            
            logger.info(f"🔍 Searching {site_name} for keyword: {keyword}")
            logger.info(f"📍 URL: {search_url}")
            
            async with AsyncWebCrawler() as crawler:
                result = await crawler.arun(
                    url=search_url,
                    wait_for=3000,
                    bypass_cache=True,
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
                    }
                )
                
                if result.success:
                    logger.info(f"✅ Successfully crawled {site_name} for {keyword}")
                    return {
                        'site': site_name,
                        'keyword': keyword,
                        'url': search_url,
                        'status': 'success',
                        'markdown': result.markdown,
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    logger.error(f"❌ Failed to crawl {site_name} for {keyword}: {result.error_message}")
                    return {
                        'site': site_name,
                        'keyword': keyword,
                        'url': search_url,
                        'status': 'failed',
                        'error': result.error_message,
                        'timestamp': datetime.now().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"❌ Exception while crawling {site_name} for {keyword}: {e}")
            return {
                'site': site_name,
                'keyword': keyword,
                'url': search_url if 'search_url' in locals() else url_template,
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    async def save_markdown_to_file(self, site_name: str, keyword: str, markdown_content: str):
        """Save markdown content to rakuten.md AND extract URLs to TXT file"""
        try:
            # ALWAYS save markdown content to rakuten.md (overwrite existing content)
            markdown_filename = "rakuten.md"
            with open(markdown_filename, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            print(f"💾 Saved markdown content to {markdown_filename}")
            
            # Also save URLs to TXT file for backward compatibility
            filename = f"{site_name}_all_urls.txt"
            
            # Extract URLs from markdown content
            extracted_urls = self.extract_urls_from_content(markdown_content, site_name)
            
            # OVERWRITE the file with simple text format (one URL per line)
            with open(filename, 'w', encoding='utf-8') as f:
                # Add header information as comments
                f.write(f"# {site_name.upper()} Search Results\n")
                f.write(f"# Site: {site_name}\n")
                f.write(f"# Keyword: {keyword}\n")
                f.write(f"# Search Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Total URLs: {len(extracted_urls)}\n")
                f.write("# URLs extracted from markdown content:\n")
                f.write("#" + "="*50 + "\n")
                
                # Write each URL on a separate line
                for url in extracted_urls:
                    f.write(f"{url}\n")
            
            logger.info(f"💾 Saved {len(extracted_urls)} URLs from {site_name} for '{keyword}' to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"❌ Failed to save content for {site_name} - {keyword}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Failed to save URLs for {site_name} - {keyword}: {e}")
            return None
    
    def extract_urls_from_content(self, content: str, site_name: str) -> List[str]:
        """Extract ALL URLs from markdown content - simple extraction without filtering"""
        # For now, return empty list - URL extraction will be handled by markdown_product_url_extractor.py
        return []
    
    async def crawl_all_sites(self) -> Dict:
        """Crawl all e-commerce sites for all keywords"""
        logger.info("🚀 Starting Generic E-commerce Crawler")
        logger.info(f"🔑 Keywords: {', '.join(self.keywords)}")
        logger.info(f"🛒 Sites: {', '.join(self.sites.keys())}")
        
        start_time = time.time()
        all_results = {}
        
        for keyword in self.keywords:
            logger.info(f"\n📝 Processing keyword: {keyword}")
            keyword_results = {}
            
            for site_name, url_template in self.sites.items():
                # Add delay between requests to be respectful
                await asyncio.sleep(2)
                
                # Crawl the site
                result = await self.search_and_crawl_site(site_name, url_template, keyword)
                keyword_results[site_name] = result
                
                # Save markdown if successful
                if result['status'] == 'success' and 'markdown' in result:
                    await self.save_markdown_to_file(site_name, keyword, result['markdown'])
            
            all_results[keyword] = keyword_results
        
        total_time = time.time() - start_time
        
        # Create summary
        summary = {
            'crawler_info': {
                'type': 'generic_ecommerce_keyword_crawler',
                'version': '1.0',
                'timestamp': datetime.now().isoformat(),
                'total_time_seconds': total_time
            },
            'keywords_processed': self.keywords,
            'sites_searched': list(self.sites.keys()),
            'results': all_results,
            'stats': {
                'total_keywords': len(self.keywords),
                'total_sites': len(self.sites),
                'total_searches': len(self.keywords) * len(self.sites),
                'successful_searches': sum(1 for keyword_data in all_results.values() 
                                         for site_data in keyword_data.values() 
                                         if site_data['status'] == 'success'),
                'failed_searches': sum(1 for keyword_data in all_results.values() 
                                     for site_data in keyword_data.values() 
                                     if site_data['status'] != 'success')
            }
        }
        
        logger.info(f"📊 Stats: {summary['stats']['successful_searches']}/{summary['stats']['total_searches']} successful")
        logger.info(f"⏱️ Total time: {total_time:.1f} seconds")
        
        return summary

    async def process_rakuten_to_json(self):
        """Process rakuten.md file and create structured JSON using Gemini"""
        try:
            # Check if rakuten.md exists
            if not os.path.exists('rakuten.md'):
                logger.warning("❌ rakuten.md file not found!")
                return None
            
            # Configure Gemini with new API key
            genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))
            model = genai.GenerativeModel(
                'gemini-1.5-flash',  # Use the stable model name
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    top_p=0.8,
                    top_k=20,
                    max_output_tokens=4096
                )
            )
            
            logger.info("🚀 Starting FAST Gemini processing for rakuten.md")
            logger.info("🤖 Using new Gemini API key with Flash model")
            
            # Read the markdown file
            with open('rakuten.md', 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Process in 100-line chunks as you requested
            lines = content.split('\n')
            chunk_size = 100  # Your preferred chunk size
            all_products = []
            
            # Detailed prompt template for proper product extraction
            prompt_template = """
You are an expert product data extractor. Extract product information from this ecommerce search results markdown content.

IMPORTANT INSTRUCTIONS:
1. Look for product listings, product names, prices, descriptions, and URLs
2. Each product should be a separate JSON object
3. Extract ALL products you can find in the content
4. If a field is not found, use null (not empty string)
5. For Japanese text, preserve the original characters
6. Return ONLY a valid JSON array, no other text

JSON Format Required:
[
  {
    "Product_Name": "Full product name (preserve Japanese characters)",
    "Product_Description": "Product description or features",
    "Price": "Price with currency symbol (e.g., '2,980円')",
    "Release_Date": "Release date if mentioned, otherwise null",
    "Volume_Size": "Size/volume if mentioned (e.g., '150ml', '30g'), otherwise null",
    "Country_of_Origin": "Country if mentioned, otherwise null",
    "Product_Image": "Image URL if found, otherwise null",
    "Brand_Name": "Brand name if identifiable, otherwise null",
    "Brand_Description": "Brand description if available, otherwise null",
    "Full_Ingredient_List": "Ingredients if listed, otherwise null",
    "New_Feature_Promotion": "Promotional text or new features, otherwise null",
    "Marketing_Materials": "Marketing text or slogans, otherwise null",
    "Packaging_Information": "Package details if mentioned, otherwise null",
    "Web_URL": "Product URL if found, otherwise null"
  }
]

Extract from this ecommerce search content:
"""
            
            # Process chunks sequentially
            total_chunks = (len(lines) + chunk_size - 1) // chunk_size
            logger.info(f"🚀 Processing {total_chunks} chunks sequentially...")
            
            for i in range(0, len(lines), chunk_size):
                chunk = lines[i:i + chunk_size]
                chunk_content = '\n'.join(chunk)
                
                if not chunk_content.strip():
                    continue
                
                chunk_num = i // chunk_size + 1
                logger.info(f"🔄 Processing chunk {chunk_num}/{total_chunks}")
                
                try:
                    response = model.generate_content(
                        prompt_template + chunk_content,
                        request_options={"timeout": 30}
                    )
                    
                    if response.text:
                        clean_response = response.text.strip()
                        if clean_response.startswith('```json'):
                            clean_response = clean_response[7:]
                        if clean_response.endswith('```'):
                            clean_response = clean_response[:-3]
                        
                        chunk_products = json.loads(clean_response.strip())
                        
                        if isinstance(chunk_products, list):
                            all_products.extend(chunk_products)
                            logger.info(f"✅ Chunk {chunk_num}: {len(chunk_products)} products")
                        else:
                            logger.warning(f"⚠️ Unexpected format in chunk {chunk_num}")
                    
                    await asyncio.sleep(0.5)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"❌ JSON error in chunk {chunk_num}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"❌ Error in chunk {chunk_num}: {e}")
                    continue
            
            # ULTRA-AGGRESSIVE deduplication and merging
            logger.info("� Applying ULTRA-AGGRESSIVE deduplication...")
            unique_products = self._ultra_aggressive_deduplicate(all_products)
            
            # Save to JSON file
            output_file = 'rakuten.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(unique_products, f, ensure_ascii=False, indent=2)
            
            logger.info(f"💾 Saved {len(unique_products)} unique products to {output_file}")
            logger.info(f"📊 Removed {len(all_products) - len(unique_products)} duplicates/incomplete entries")
            return unique_products
            
        except Exception as e:
            logger.error(f"❌ Error in LLM processing: {e}")
            return None

    def _ultra_aggressive_deduplicate(self, products):
        """Smart deduplication - removes true duplicates but keeps variant products"""
        try:
            logger.info(f"🔥 Starting with {len(products)} raw products")
            
            # Strategy 1: Filter out obviously invalid/empty products
            valid_products = []
            for product in products:
                if not isinstance(product, dict):
                    continue
                
                name = str(product.get('Product_Name', '')).strip()
                price = str(product.get('Price', '')).strip()
                desc = str(product.get('Product_Description', '')).strip()
                
                # Skip if no meaningful data
                if (not name or name.lower() in ['null', 'none', '']) and \
                   (not price or price.lower() in ['null', 'none', '']) and \
                   (not desc or desc.lower() in ['null', 'none', '']):
                    continue
                
                # Skip if name is just a number or too short (but allow 2 chars for Japanese)
                if name and len(name) < 2:
                    continue
                
                valid_products.append(product)
            
            logger.info(f"🔄 After filtering invalid: {len(valid_products)} products")
            
            # Strategy 2: Smart grouping - only merge EXACT duplicates, keep variants
            exact_duplicates = {}
            
            for product in valid_products:
                name = str(product.get('Product_Name', '')).strip().lower()
                price = str(product.get('Price', '')).strip()
                url = str(product.get('Web_URL', '')).strip()
                
                # Create exact match key - name + price + URL for precise duplicate detection
                exact_key = None
                
                if name and price and price.lower() not in ['null', 'none', '']:
                    # Use name + price for exact matching
                    exact_key = f"{name}|{price}"
                    
                    # If we also have URL, make it even more specific
                    if url and url.lower() not in ['null', 'none', ''] and 'item.rakuten.co.jp' in url:
                        exact_key = f"{name}|{price}|{url}"
                
                if exact_key and exact_key in exact_duplicates:
                    # True duplicate found - merge data
                    existing = exact_duplicates[exact_key]
                    merged = self._merge_product_data(existing, product)
                    exact_duplicates[exact_key] = merged
                elif exact_key:
                    # New unique product
                    exact_duplicates[exact_key] = product
                else:
                    # Product without enough data for comparison - keep it anyway
                    # Use a unique key based on index to avoid losing it
                    fallback_key = f"fallback_{len(exact_duplicates)}"
                    exact_duplicates[fallback_key] = product
            
            logger.info(f"🔄 After exact duplicate removal: {len(exact_duplicates)} unique products")
            
            # Strategy 3: Final quality filter - keep more products
            final_products = []
            for product in exact_duplicates.values():
                name = str(product.get('Product_Name', '')).strip()
                
                # More lenient criteria - keep products with meaningful names
                if name and len(name) >= 2 and name.lower() not in ['null', 'none', '']:
                    # Clean up the product data
                    cleaned_product = self._clean_product_data(product)
                    final_products.append(cleaned_product)
            
            # Sort by name for consistent output
            final_products.sort(key=lambda x: str(x.get('Product_Name', '')).lower())
            
            logger.info(f"🎯 FINAL RESULT: {len(final_products)} unique products (kept more variants)")
            return final_products
            
        except Exception as e:
            logger.error(f"❌ Error in smart deduplication: {e}")
            return products

    def _extract_core_name(self, name):
        """Extract core product name by removing size/color/variation indicators"""
        import re
        
        # Convert to lower case for processing
        core = name.lower()
        
        # Remove common size indicators
        core = re.sub(r'\b\d+\s*(ml|g|kg|oz|lb|l|liter|gram|kilogram)\b', '', core)
        core = re.sub(r'\b\d+\s*[×x]\s*\d+\b', '', core)  # dimensions
        core = re.sub(r'\b\d+\s*個\b', '', core)  # Japanese count
        core = re.sub(r'\b\d+\s*本\b', '', core)  # Japanese count
        
        # Remove color indicators
        color_words = ['black', 'white', 'red', 'blue', 'green', 'yellow', 'pink', 'purple', 'gray', 'grey', 'brown']
        for color in color_words:
            core = re.sub(rf'\b{color}\b', '', core)
        
        # Remove parenthetical information
        core = re.sub(r'\([^)]*\)', '', core)
        core = re.sub(r'\[[^\]]*\]', '', core)
        
        # Clean up extra spaces
        core = ' '.join(core.split())
        
        return core.strip()

    def _clean_product_data(self, product):
        """Clean up product data removing null/empty values"""
        cleaned = {}
        
        for key, value in product.items():
            if value is None or value == '' or str(value).strip() == '' or str(value).lower() == 'null':
                cleaned[key] = None
            else:
                # Clean string values
                cleaned_value = str(value).strip()
                cleaned[key] = cleaned_value if cleaned_value else None
        
        return cleaned

    def _deduplicate_and_merge_products(self, products):
        """Deduplicate products and merge incomplete data intelligently"""
        try:
            # Dictionary to store unique products by key
            unique_dict = {}
            
            for product in products:
                if not isinstance(product, dict):
                    continue
                
                # Create a unique key based on product name, price, and volume
                # This handles cases where URLs might be different but product is same
                name = str(product.get('Product_Name', '')).strip()
                price = str(product.get('Price', '')).strip() 
                volume = str(product.get('Volume_Size', '')).strip()
                
                # Skip products with no meaningful data
                if not name and not price:
                    continue
                
                # Skip if name is null or None
                if name.lower() in ['null', 'none', '']:
                    continue
                
                # Create unique key - prioritize name, then add price and volume for disambiguation
                unique_key = name.lower()
                if price and price.lower() not in ['null', 'none', '']:
                    unique_key += f"|{price.lower()}"
                if volume and volume.lower() not in ['null', 'none', '']:
                    unique_key += f"|{volume.lower()}"
                
                if unique_key in unique_dict:
                    # Merge with existing product - keep non-null/non-empty values
                    existing = unique_dict[unique_key]
                    merged = self._merge_product_data(existing, product)
                    unique_dict[unique_key] = merged
                else:
                    # New product
                    unique_dict[unique_key] = product
            
            # Convert back to list and filter out products with only URLs
            final_products = []
            for product in unique_dict.values():
                # Only include products that have substantial data (not just URL)
                name = str(product.get('Product_Name', '')).strip()
                if name and name.lower() not in ['null', 'none', '']:
                    final_products.append(product)
            
            # Sort by product name for consistent output
            final_products.sort(key=lambda x: str(x.get('Product_Name', '')).lower())
            
            return final_products
            
        except Exception as e:
            logger.error(f"❌ Error in deduplication: {e}")
            return products  # Return original if deduplication fails

    def _merge_product_data(self, existing, new):
        """Merge two product dictionaries, preferring non-null/non-empty values"""
        try:
            merged = existing.copy()
            
            for key, value in new.items():
                # Skip if new value is null, empty, or 'null' string
                if value is None or value == '' or value == 'null':
                    continue
                
                # If existing value is null/empty, use new value
                existing_value = merged.get(key)
                if (existing_value is None or existing_value == '' or 
                    existing_value == 'null' or str(existing_value).strip() == ''):
                    merged[key] = value
                    continue
                
                # For specific fields, prefer longer/more detailed values
                if key in ['Product_Description', 'Brand_Description', 'Full_Ingredient_List', 
                          'Marketing_Materials', 'Packaging_Information', 'New_Feature_Promotion']:
                    if len(str(value)) > len(str(existing_value)):
                        merged[key] = value
                
                # For URLs, prefer actual item URLs over search URLs
                elif key == 'Web_URL':
                    if 'item.rakuten.co.jp' in str(value) and 'search.rakuten.co.jp' in str(existing_value):
                        merged[key] = value
                    elif 'item.rakuten.co.jp' not in str(existing_value) and 'item.rakuten.co.jp' in str(value):
                        merged[key] = value
                
                # For other fields, keep existing if both have values
                # Could add more sophisticated merging logic here if needed
                
            return merged
            
        except Exception as e:
            logger.error(f"❌ Error merging product data: {e}")
            return existing

async def main():
    """Main function to run the generic e-commerce crawler"""
    
    # Ask for keywords in terminal prompt
    try:
        print("🔑 E-commerce Keyword Crawler")
        print("Enter keywords (comma-separated):")
        keywords_input = input("Keywords: ").strip()
        
        if not keywords_input:
            print("❌ No keywords provided!")
            return
            
        keywords = [kw.strip() for kw in keywords_input.split(',') if kw.strip()]
        print(f"🔑 Using keywords: {', '.join(keywords)}")
        
        # Ask for site selection
        print("\n🌐 Enter websites to crawl:")
        print("Enter one per line (press Enter on empty line to finish):")
        
        sites_to_crawl = set()
        
        def normalize_site_name(site_input):
            """Normalize various site input formats to standard names"""
            site_lower = site_input.lower().strip()
            
            # Rakuten variations
            if any(keyword in site_lower for keyword in ['rakuten', 'item.rakuten', 'search.rakuten']):
                return 'rakuten'
            
            # Amazon variations  
            elif any(keyword in site_lower for keyword in ['amazon', 'amazon.co.jp', 'amazon.com']):
                return 'amazon'
            
            # Yahoo variations
            elif any(keyword in site_lower for keyword in ['yahoo', 'shopping.yahoo', 'yahoo.co.jp']):
                return 'yahoo'
            
            # AU PAY variations
            elif any(keyword in site_lower for keyword in ['aupay', 'au pay', 'wowma', 'au.com']):
                return 'aupay'
            
            # Cosme variations
            elif any(keyword in site_lower for keyword in ['cosme', 'cosmeet.cosme', 'cosme.net']):
                return 'cosme'
            
            return None
        
        while True:
            site_input = input("Website: ").strip()
            if not site_input:
                break
            
            normalized_site = normalize_site_name(site_input)
            
            if normalized_site:
                sites_to_crawl.add(normalized_site)
                print(f"✅ Added {normalized_site} (from: {site_input})")
            else:
                print(f"⚠️  '{site_input}' not recognized. Try: rakuten, amazon.com, yahoo, aupay, cosme")
        
        # Set boolean flags based on selected sites
        rakuten_enabled = 'rakuten' in sites_to_crawl
        amazon_enabled = 'amazon' in sites_to_crawl
        yahoo_enabled = 'yahoo' in sites_to_crawl
        aupay_enabled = 'aupay' in sites_to_crawl
        cosme_enabled = 'cosme' in sites_to_crawl
        
        # Default to Rakuten if nothing selected
        if not sites_to_crawl:
            print("⚠️  No sites selected, defaulting to Rakuten")
            rakuten_enabled = True
            sites_to_crawl.add('rakuten')
        
        print(f"🎯 Selected sites: {', '.join(sorted(sites_to_crawl))}")
        
    except (EOFError, KeyboardInterrupt):
        print("\n❌ Input cancelled!")
        return
    
    print(f"\n🚀 Starting crawl with keywords: {', '.join(keywords)}")
    
    # Create and run crawler with selected sites
    crawler = GenericEcommerceCrawler(
        keywords=keywords,
        rakuten=rakuten_enabled,
        amazon=amazon_enabled,
        yahoo=yahoo_enabled,
        aupay=aupay_enabled,
        cosme=cosme_enabled
    )
    results = await crawler.crawl_all_sites()
    
    # Print summary
    print(f"\n📋 SUMMARY:")
    print(f"  • Keywords processed: {results['stats']['total_keywords']}")
    print(f"  • Sites searched: {results['stats']['total_sites']}")
    print(f"  • Successful searches: {results['stats']['successful_searches']}")
    print(f"  • Failed searches: {results['stats']['failed_searches']}")
    
    # Show only the files that were created
    created_files = []
    if rakuten_enabled: created_files.append("rakuten_all_urls.txt")
    if amazon_enabled: created_files.append("amazon_all_urls.txt")
    if yahoo_enabled: created_files.append("yahoo_all_urls.txt")
    if aupay_enabled: created_files.append("aupay_all_urls.txt")
    if cosme_enabled: created_files.append("cosme_all_urls.txt")
    
    print(f"  • Website URL files: {', '.join(created_files)}")
    print(f"  • Each TXT file contains extracted URLs for that site")

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Markdown Product URL Extractor
Reads markdown files (e.g., rakuten.md) and uses AI to extract ONLY product URLs in chunks
This is more accurate than regex extraction as it uses AI to identify actual product URLs
Uses Gemini for AI-powered URL extraction with website analysis
"""                                                                               

import json
import os
import time
from typing import List, Dict, Any
import google.generativeai as genai
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Gemini Configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
# Force use the correct model name regardless of environment variable
GEMINI_MODEL_NAME = 'gemini-2.5-flash-lite'

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in environment variables")

class MarkdownProductURLExtractor:
    def __init__(self):
        """Initialize the markdown product URL extractor with Gemini."""
        # Configure Gemini
        genai.configure(api_key=GEMINI_API_KEY)
        self.model = genai.GenerativeModel(GEMINI_MODEL_NAME)
        
        print(f"🤖 Initialized with Gemini model: {GEMINI_MODEL_NAME}")
        
        # Enhanced prompt with website analysis for better product URL extraction
        self.extraction_prompt = """You are an expert e-commerce website analyzer and product URL extractor.

TASK OVERVIEW:
1. First, analyze the provided e-commerce website content to understand its structure and URL patterns
2. Then extract ALL product page URLs that allow customers to view and purchase individual products
3. IMPORTANT: There are approximately 60 product URLs total in this content - find ALL of them!

STEP 1 - WEBSITE ANALYSIS:
Analyze the domain and content to understand:
- What type of e-commerce site this is (marketplace, brand store, etc.)
- Common URL patterns for product pages on this site
- How product IDs and categories are structured in URLs
- Any unique characteristics of this site's product URL format

STEP 2 - PRODUCT URL EXTRACTION:
Based on your analysis, extract ALL URLs that are individual product pages where customers can:
✅ View detailed product information
✅ See product images, descriptions, and specifications  
✅ Add the product to cart or purchase it
✅ Select product variants (size, color, etc.)

INCLUDE these types of URLs:
✅ Direct product detail pages with specific product IDs
✅ Product variant pages (different colors, sizes, models)
✅ Product pages in any category or subcategory
✅ Mobile or alternate versions of product pages
✅ Product pages with tracking parameters (utm, ref, etc.)


ANALYSIS CONTENT:
{content}

OUTPUT FORMAT:
Return your response as valid JSON with this exact structure:
{{
  "website_analysis": {{
    "domain": "detected domain name",
    "site_type": "marketplace/brand_store/etc",
    "product_url_patterns": ["pattern1", "pattern2"],
    "unique_characteristics": "brief description of site-specific patterns"
  }},
  "product_urls": ["url1", "url2", "url3", ...]
}}

Be thorough and comprehensive - missing product URLs means lost business opportunities!"""

    def load_markdown_file(self, file_path: str) -> str:
        """Load markdown content from file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                print(f"✅ Loaded markdown file: {file_path}")
                print(f"📄 Content length: {len(content):,} characters")
                return content
        except FileNotFoundError:
            print(f"❌ File not found: {file_path}")
            return ""
        except Exception as e:
            print(f"❌ Error loading file {file_path}: {str(e)}")
            return ""

    def split_content_into_chunks(self, content: str) -> List[str]:
        """No longer needed - we process the entire file at once."""
        # Return the entire content as a single "chunk"
        return [content] if content.strip() else []

    def extract_urls_from_content(self, content: str) -> List[str]:
        """Extract product URLs by first getting all URLs, then filtering with AI."""
        try:
            print(f"🔄 Processing entire file ({len(content):,} characters)...")
            
            # Step 1: Extract ALL HTTP(S) URLs from markdown using regex
            import re
            print("🔗 Extracting all HTTP(S) URLs from content...")
            raw_urls = re.findall(r'https?://[^\s)"<>]+', content)
            
            # Step 2: Deduplicate while preserving order
            seen = set()
            all_urls = []
            for url in raw_urls:
                clean_url = url.rstrip('.,;:!?"\')')  # Clean trailing punctuation
                if clean_url not in seen:
                    seen.add(clean_url)
                    all_urls.append(clean_url)
            
            print(f"📊 Found {len(raw_urls)} total URLs, {len(all_urls)} unique URLs")
            
            if not all_urls:
                print("❌ No URLs found in content")
                return []
            
            # 🐛 DEBUG: Save all URLs before sending to GPT-4o
            debug_data = {
                "extraction_timestamp": datetime.now().isoformat(),
                "total_raw_urls": len(raw_urls),
                "unique_urls_count": len(all_urls),
                "all_candidate_urls": all_urls
            }
            
            with open('product_urls_debug.json', 'w', encoding='utf-8') as f:
                json.dump(debug_data, f, indent=2, ensure_ascii=False)
            print(f"🐛 DEBUG: Saved {len(all_urls)} candidate URLs to product_urls_debug.json")
            
            # 🐛 DEBUG: Show first 10 URLs being sent to GPT-4o
            print(f"🐛 DEBUG: First 10 URLs being sent to GPT-4o:")
            for i, url in enumerate(all_urls[:10]):
                print(f"   {i+1}. {url}")
            if len(all_urls) > 10:
                print(f"   ... and {len(all_urls) - 10} more URLs")
            
            # Step 3: Process URLs in chunks of 50 for better performance and fewer safety issues
            chunk_size = 50
            all_product_urls = []
            
            print(f"🔄 Processing {len(all_urls)} URLs in chunks of {chunk_size}...")
            
            for i in range(0, len(all_urls), chunk_size):
                chunk_urls = all_urls[i:i + chunk_size]
                chunk_num = (i // chunk_size) + 1
                total_chunks = (len(all_urls) + chunk_size - 1) // chunk_size
                
                print(f"📦 Processing chunk {chunk_num}/{total_chunks} ({len(chunk_urls)} URLs)...")
                
                # Create chunk data for GPT-4o
                chunk_data = {
                    "chunk_info": {
                        "chunk_number": chunk_num,
                        "total_chunks": total_chunks,
                        "urls_in_chunk": len(chunk_urls)
                    },
                    "urls": chunk_urls
                }
                
                # Enhanced prompt with safer URL handling to avoid safety filters
                # Instead of sending raw URLs, we'll sanitize them better
                url_descriptions = []
                for j, url in enumerate(chunk_urls, 1):
                    # More aggressive sanitization to avoid safety filters
                    clean_url = url.split('?')[0]  # Remove query parameters
                    clean_url = clean_url.split('#')[0]  # Remove fragments
                    # Further sanitize by removing potentially problematic path components
                    if '/ref=' in clean_url:
                        clean_url = clean_url.split('/ref=')[0]
                    url_descriptions.append(f"{j}. {clean_url}")
                
                urls_text = "\n".join(url_descriptions)
                
                prompt = f"""You are analyzing e-commerce URLs to identify product pages.

TASK: Review these {len(chunk_urls)} URLs and identify which ones are individual product pages.

CRITERIA for product pages:
✅ URLs that show individual items for sale
✅ Have specific product identifiers (like /dp/, /item/, /product/)
✅ Lead to pages where customers can purchase items
✅ Display specific product details

EXCLUDE:
❌ Search results or category pages
❌ Navigation or account pages
❌ Media files or resources

URL LIST:
{urls_text}

Return only the numbers (1, 2, 3, etc.) of URLs that are product pages.
Output as JSON: {{"product_url_numbers": [1, 5, 8, ...]}}"""

                print(f"🤖 Sending chunk {chunk_num} ({len(chunk_urls)} URLs) to Gemini...")
                print(f"🐛 DEBUG: Chunk prompt length: {len(prompt)} characters")
                
                try:
                    # Step 4: Call Gemini API to filter product URLs for this chunk
                    response = self.model.generate_content(
                        prompt,
                        generation_config=genai.types.GenerationConfig(
                            temperature=0.1,
                            max_output_tokens=8192,
                            response_mime_type="application/json"
                        )
                    )
                    
                    # Handle safety filter issues - if blocked, skip this chunk
                    if not response.candidates or len(response.candidates) == 0:
                        print(f"⚠️ Chunk {chunk_num}: No candidates returned, skipping this chunk...")
                        continue
                        
                    candidate = response.candidates[0]
                    if candidate.finish_reason == 2:  # SAFETY
                        print(f"⚠️ Chunk {chunk_num}: Response blocked by safety filters, skipping this chunk...")
                        continue
                    
                    if not response.text:
                        print(f"⚠️ Chunk {chunk_num}: Empty response, skipping this chunk...")
                        continue
                        
                    response_text = response.text.strip()
                    print(f"🐛 DEBUG: Gemini chunk {chunk_num} response length: {len(response_text)} characters")
                    
                    # Step 5: Parse JSON response for this chunk
                    try:
                        # Clean the response (remove any markdown formatting)
                        if response_text.startswith('```'):
                            lines = response_text.split('\n')
                            json_lines = [line for line in lines if not line.strip().startswith('```')]
                            response_text = '\n'.join(json_lines).strip()
                        
                        # Parse JSON
                        data = json.loads(response_text)
                        
                        if isinstance(data, dict) and 'product_url_numbers' in data:
                            # Get the URLs based on the returned numbers
                            url_numbers = data['product_url_numbers']
                            chunk_product_urls = []
                            for num in url_numbers:
                                if 1 <= num <= len(chunk_urls):
                                    chunk_product_urls.append(chunk_urls[num - 1])  # Convert to 0-based index
                            
                            print(f"✅ Chunk {chunk_num}: Found {len(chunk_product_urls)} product URLs")
                            all_product_urls.extend(chunk_product_urls)
                        elif isinstance(data, dict) and 'product_urls' in data:
                            # Fallback to old format if returned
                            chunk_product_urls = data['product_urls']
                            print(f"✅ Chunk {chunk_num}: Found {len(chunk_product_urls)} product URLs")
                            all_product_urls.extend(chunk_product_urls)
                        elif isinstance(data, list):
                            print(f"✅ Chunk {chunk_num}: Found {len(data)} product URLs")
                            all_product_urls.extend(data)
                        else:
                            print(f"⚠️ Chunk {chunk_num}: Unexpected response format")
                            print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                            
                    except json.JSONDecodeError as e:
                        print(f"❌ Chunk {chunk_num} JSON parsing error: {str(e)}")
                        print(f"Response preview: {response_text[:200]}...")
                        
                except Exception as e:
                    print(f"❌ Chunk {chunk_num} API error: {str(e)}")
                
                # Add small delay between chunks to avoid rate limits
                if chunk_num < total_chunks:
                    time.sleep(2)
            
            print(f"🎯 Total product URLs found across all chunks: {len(all_product_urls)}")
            
            # Final deduplication and cleaning
            unique_product_urls = list(set(all_product_urls))
            unique_product_urls.sort()
            
            print(f"🧹 After deduplication: {len(unique_product_urls)} unique product URLs")
            return unique_product_urls
                
        except Exception as e:
            print(f"❌ API error: {str(e)}")
            return []

    def extract_product_urls_from_markdown(self, markdown_file: str, output_file: str):
        """Main function to extract product URLs from markdown file."""
        print("🚀 Starting Markdown Product URL Extraction...")
        print(f"📄 Input: {markdown_file}")
        print(f"💾 Output: {output_file}")
        
        # Load markdown content
        content = self.load_markdown_file(markdown_file)
        if not content:
            print("❌ No content to process")
            return
        
        print(f"🔄 Processing entire file with Gemini AI + website analysis (chunked)")
        print(f"📊 Content size: {len(content):,} characters")
        
        # Extract URLs from entire content
        try:
            all_product_urls = self.extract_urls_from_content(content)
            
            if all_product_urls:
                # Remove duplicates and clean URLs
                unique_urls = list(set(all_product_urls))
                unique_urls.sort()
                
                # Determine site name from markdown file name
                site_name = os.path.splitext(os.path.basename(markdown_file))[0]
                
                output_data = {
                    "extraction_metadata": {
                        "source_file": markdown_file,
                        "site_name": site_name,
                        "extraction_method": "gemini_ai_with_website_analysis",
                        "ai_model": GEMINI_MODEL_NAME,
                        "content_length": len(content),
                        "total_urls_found": len(unique_urls),
                        "extraction_timestamp": datetime.now().isoformat()
                    },
                    "product_urls": unique_urls
                }
                
                # Save as JSON
                try:
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(output_data, f, ensure_ascii=False, indent=2)
                    
                    print(f"✅ Successfully extracted {len(unique_urls)} product URLs")
                    print(f"📊 Processing Summary:")
                    print(f"   - Source file: {markdown_file}")
                    print(f"   - Content length: {len(content):,} characters")
                    print(f"   - Product URLs found: {len(unique_urls)}")
                    print(f"   - Duplicates removed: {len(all_product_urls) - len(unique_urls)}")
                    print(f"   - Saved to: {output_file}")
                    
                    # Show sample URLs
                    if unique_urls:
                        print(f"\n📋 Sample URLs (first 5):")
                        for i, url in enumerate(unique_urls[:5], 1):
                            print(f"   {i}. {url}")
                        if len(unique_urls) > 5:
                            print(f"   ... and {len(unique_urls) - 5} more")
                    
                except Exception as e:
                    print(f"❌ Error saving results: {str(e)}")
            else:
                print("❌ No product URLs were extracted")
                
        except Exception as e:
            print(f"❌ Failed to process file: {str(e)}")
            return

def main():
    """Main function to run the markdown product URL extraction with Gemini AI."""
    print("🔍 Markdown Product URL Extractor (Gemini AI + Website Analysis)")
    print("=" * 65)
    
    # Initialize extractor
    extractor = MarkdownProductURLExtractor()
    
    # Default files - can be customized
    markdown_file = "rakuten.md"
    output_file = "rakuten_product_urls_from_markdown.json"
    
    # Check if input file exists
    if not os.path.exists(markdown_file):
        print(f"❌ Input file {markdown_file} not found")
        print("   Please make sure the markdown file exists")
        
        # List available markdown files
        md_files = [f for f in os.listdir('.') if f.endswith('.md')]
        if md_files:
            print(f"   Available markdown files: {', '.join(md_files)}")
        return
    
    # Run extraction
    extractor.extract_product_urls_from_markdown(markdown_file, output_file)
    
    print("\n✅ Extraction completed!")

if __name__ == "__main__":
    main()

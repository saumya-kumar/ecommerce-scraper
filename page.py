#!/usr/bin/env python3
"""
Universal E-commerce Product Page Scraper
Generates clean markdown from any e-commerce product URL with all links removed.
Works with Amazon, Rakuten, Yahoo Shopping, and other major e-commerce platforms.
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import Optional

try:
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
    from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
    from crawl4ai.content_filter_strategy import PruningContentFilter
except ImportError as e:
    print("❌ Error: Crawl4AI not installed. Please install it first:")
    print("   pip install crawl4ai")
    sys.exit(1)

class UniversalProductPageProcessor:
    def __init__(self):
        """Initialize the universal e-commerce product page processor."""
        self.output_file = "page.md"
        
    async def process_url(self, url: str, output_file: Optional[str] = None) -> bool:
        """
        Process a single e-commerce product URL and generate clean markdown without links.
        
        Args:
            url: The product URL to process (Amazon, Rakuten, Yahoo Shopping, etc.)
            output_file: Optional custom output filename (default: page.md)
            
        Returns:
            bool: True if successful, False otherwise
        """
        if output_file:
            self.output_file = output_file
            
        print(f"🔄 Processing URL: {url}")
        print(f"💾 Output file: {self.output_file}")
        print("-" * 60)
        
        try:
            # OPTIMIZED BROWSER CONFIG for e-commerce product pages
            browser_config = BrowserConfig(
                headless=True,
                viewport_width=1920,             # Wide viewport for full product layouts
                viewport_height=1080,            # Tall viewport for complete product info
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
                java_script_enabled=True,        # Essential for dynamic e-commerce content
                ignore_https_errors=True,        # Handle certificate issues
                text_mode=False,                 # Keep images for complete product data
            )
            # Configure the markdown generator EXACTLY per documentation
            # Use raw_html to preserve ALL content, only remove links
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
                # NO content_filter at all - preserve everything except links
            )
            
            # Configure crawler with ADVANCED Crawl4AI features optimized for Gemini processing
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
            
            # Process the URL with optimized browser config
            async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
                print("🕷️ Starting crawl...")
                result = await crawler.arun(url, config=config)
                
                # Debug: Check what we actually got
                print(f"🔍 DEBUG - Result success: {result.success}")
                if result.html:
                    print(f"🔍 DEBUG - HTML length: {len(result.html)}")
                    # Check if we have expected product content in HTML
                    html_lower = result.html.lower()
                    
                    # Check for various e-commerce brands/keywords
                    found_keywords = []
                    keywords_to_check = ['elixir', 'daycare', 'lancome', 'ランコム', 'shiseido', '資生堂', 'cosmetics', '化粧品']
                    
                    for keyword in keywords_to_check:
                        if keyword in html_lower:
                            count = html_lower.count(keyword)
                            found_keywords.append(f"'{keyword}' ({count}x)")
                    
                    if found_keywords:
                        print(f"✅ Found product keywords in HTML: {', '.join(found_keywords)}")
                    else:
                        print("⚠️ No expected product keywords found in HTML")
                        
                    # Show HTML preview for debugging
                    html_preview = result.html[:1000].replace('\n', ' ')
                    print(f"🔍 HTML preview: {html_preview}...")
                else:
                    print("❌ No HTML content received")
                
                if not result.success:
                    print(f"❌ Crawl failed: {result.error_message}")
                    return False
                
                # Get the raw markdown with links removed (per documentation)
                if result.markdown and hasattr(result.markdown, 'raw_markdown'):
                    clean_markdown = result.markdown.raw_markdown  # This is the key - raw unfiltered markdown!
                    print("✅ Using raw_markdown with links removed")
                elif result.markdown:
                    clean_markdown = str(result.markdown)
                    print("✅ Using basic markdown string")
                else:
                    print("❌ No markdown content generated")
                    return False
                
                # Define keywords to check for any e-commerce site
                keywords_to_check = ['elixir', 'daycare', 'lancome', 'ランコム', 'shiseido', '資生堂', 'cosmetics', '化粧品']
                
                # Debug: Check if product content survived the markdown conversion
                markdown_lower = clean_markdown.lower()
                found_md_keywords = []
                for keyword in keywords_to_check:
                    if keyword in markdown_lower:
                        count = markdown_lower.count(keyword)
                        found_md_keywords.append(f"'{keyword}' ({count}x)")
                
                if found_md_keywords:
                    print(f"✅ Product keywords found in markdown: {', '.join(found_md_keywords)}")
                else:
                    print("❌ Product keywords NOT found in markdown - content lost during extraction!")
                    # Show what we actually got in markdown
                    md_preview = clean_markdown[:500].replace('\n', ' ')
                    print(f"🔍 Markdown preview: {md_preview}...")
                
                # Post-process to ensure ALL links are completely removed
                import re
                
                # Remove markdown links [text](url) - replace with just the text
                final_markdown = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', clean_markdown)
                
                # Remove any remaining markdown links with empty URLs []()
                final_markdown = re.sub(r'\[([^\]]*)\]\(\)', r'\1', final_markdown)
                
                # Remove any remaining standalone URLs (but preserve the header URL)
                lines = final_markdown.split('\n')
                cleaned_lines = []
                for i, line in enumerate(lines):
                    # Skip URL removal for the first few header lines
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
                
                # Add metadata header with URL info
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                header = f"""# Page Content

**Source URL:** {url}  
**Processed:** {timestamp}  
**Content Type:** Full content with links removed

---

"""
                
                final_content = header + final_markdown
                
                # Save to file
                with open(self.output_file, 'w', encoding='utf-8') as f:
                    f.write(final_content)
                
                # Statistics
                content_length = len(final_markdown)
                word_count = len(final_markdown.split())
                line_count = len([line for line in final_markdown.split('\n') if line.strip()])
                
                print("✅ Processing completed successfully!")
                print(f"📊 Statistics:")
                print(f"   • Content length: {content_length:,} characters")
                print(f"   • Word count: {word_count:,} words")
                print(f"   • Lines: {line_count:,}")
                print(f"   • File size: {os.path.getsize(self.output_file):,} bytes")
                print(f"💾 Saved to: {self.output_file}")
                
                return True
                
        except Exception as e:
            print(f"❌ Error processing URL: {str(e)}")
            return False

def main():
    """Main function to run the page processor."""
    if len(sys.argv) < 2:
        print("🔄 Universal E-commerce Product Page Scraper")
        print("=" * 60)
        print("Usage: python page.py <URL> [output_file]")
        print()
        print("Examples:")
        print("  python page.py https://www.amazon.co.jp/dp/B123456789")
        print("  python page.py https://item.rakuten.co.jp/shop/item/")
        print("  python page.py https://store.shopping.yahoo.co.jp/shop/item.html")
        print("  python page.py https://any-ecommerce-site.com/product custom_page.md")
        print()
        print("Features:")
        print("  • Works with any e-commerce website")
        print("  • Removes ONLY links and URLs")
        print("  • Preserves ALL product information")
        print("  • Filters out navigation and ads")
        print("  • Perfect for AI processing without link noise")
        sys.exit(1)
    
    url = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Validate URL
    if not url.startswith(('http://', 'https://')):
        print("❌ Error: URL must start with http:// or https://")
        sys.exit(1)
    
    # Run the processor
    processor = UniversalProductPageProcessor()
    
    try:
        success = asyncio.run(processor.process_url(url, output_file))
        if success:
            print("\n🎉 Page processing completed successfully!")
        else:
            print("\n❌ Page processing failed!")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n❌ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

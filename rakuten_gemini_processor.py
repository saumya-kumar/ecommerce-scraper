import json
import os
import time
from typing import List, Dict, Any
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class RakutenGeminiProcessor:
    def __init__(self):
        """Initialize the Gemini processor with API configuration."""
        # Configure Gemini API
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        genai.configure(api_key=api_key)
        
        # Use Gemini 2.5 Flash Lite as it's the working model
        self.model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
        # Processing configuration optimized for enhanced, clean content
        self.chunk_size = 5  # Increased to 5 as suggested - handles more products per chunk
        self.delay_between_requests = 25  # Slightly reduced delay due to better content quality
        
        # Optimized prompt template for link-free, clean markdown content
        self.prompt_template = """You are an expert e-commerce data extractor specializing in Japanese cosmetics and beauty products. 

IMPORTANT CONTEXT:
- You will receive {num_products} separate product pages from Rakuten/Amazon
- The content has been DOUBLE-CLEANED: ALL links removed by advanced processing, navigation filtered out, only product content remains
- Each product page contains PURE PRODUCT INFORMATION without any clickable links or distracting elements
- The markdown has been processed with advanced scrolling and link removal to capture clean, focused content
- Focus on the rich product details that are now clearly visible without link noise

CRITICAL INSTRUCTIONS:
1. Each product page is clearly marked with "=== PRODUCT X - URL: [url] ===" 
2. Extract information ONLY from the content between these markers for each product
3. Return EXACTLY {num_products} products in the JSON array - one for each URL provided
4. Match each product to its specific URL
5. Do NOT mix information between different products

CONTENT QUALITY NOTES:
- The markdown content is CLEAN (no navigation links, ads, or distractions)
- Product information is COMPLETE (advanced crawling captured lazy-loaded content)
- Text is FOCUSED (only product-related content remains)
- Look for detailed product specifications, ingredients, descriptions, and pricing

{markdown_content}

EXTRACTION FIELDS (prioritize based on clean content available):

**REQUIRED FIELDS** (must extract if available):
- Product Name: The main product title (look for large headings, product titles)
- Brand Name: Brand/manufacturer name (often in titles or prominently displayed)
- Price: Current selling price (look for ¥ symbols, price displays)
- Web URL: Use the EXACT URL provided in the marker for that product

**HIGH PRIORITY FIELDS** (extract if clearly present):
- Product Description: Main product description, benefits, features
- Volume/Size: Product size/volume (ml, g, oz, pieces, etc.)
- Full Ingredient List: Complete ingredients if listed (common for cosmetics)

**ADDITIONAL FIELDS** (extract if found, null if not available):
- Release Date: Product launch/release date if mentioned
- Country of Origin: Manufacturing country (Made in Japan, etc.)
- Brand Description: Information about the brand itself
- New Feature Promotion: Special features, "NEW" labels, promotional text
- Marketing Materials: Marketing claims, benefits, selling points
- Packaging Information: Package details, container type, refillable, etc.

EXTRACTION STRATEGY FOR CLEAN CONTENT:
1. Look for clear product sections and headings
2. Identify price information (¥ symbols, numerical prices)
3. Find ingredient lists (often detailed for cosmetics)
4. Extract size/volume information (numbers + units)
5. Capture brand and product names from titles/headers
6. Look for promotional text and marketing claims

OUTPUT FORMAT:
Return EXACTLY {num_products} products as a JSON array:

[
  {{
    "Product Name": "complete product name from product 1",
    "Product Description": "detailed description from product 1", 
    "Price": "¥X,XXX or price from product 1",
    "Release Date": null,
    "Volume/Size": "XX ml/g/pieces from product 1",
    "Country of Origin": "Japan/country from product 1",
    "Brand Name": "brand name from product 1",
    "Brand Description": "brand info from product 1 or null",
    "Full Ingredient List": "complete ingredients from product 1 or null",
    "New Feature Promotion": "promotional features from product 1 or null",
    "Marketing Materials": "marketing text from product 1 or null",
    "Packaging Information": "package details from product 1 or null",
    "Web URL": "exact_url_from_product_1_marker"
  }},
  {{
    "Product Name": "complete product name from product 2",
    ...
  }}
]

QUALITY ASSURANCE:
- The clean content should make extraction easier and more accurate
- Focus on the rich product details now clearly visible
- Use the improved content quality to provide complete, detailed extractions
- Validate that each product corresponds to its specific URL marker

VALIDATION: Return exactly {num_products} products matching the {num_products} clean product pages provided."""

    def load_rakuten_data(self, file_path: str) -> List[Dict[str, Any]]:
        """Load data from rakuten.json file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                products = data.get('products', [])
                print(f"✅ Loaded {len(products)} products from {file_path}")
                return products
        except Exception as e:
            print(f"❌ Error loading file {file_path}: {str(e)}")
            return []

    def remove_links_from_markdown(self, markdown_content: str) -> str:
        """Remove all links from markdown content to clean it for Gemini processing."""
        if not markdown_content:
            return markdown_content
        
        import re
        
        # Remove markdown links: [text](url) -> text
        markdown_content = re.sub(r'\[([^\]]*)\]\([^)]+\)', r'\1', markdown_content)
        
        # Remove markdown images: ![alt](url) -> (removed)
        markdown_content = re.sub(r'!\[[^\]]*\]\([^)]+\)', '', markdown_content)
        
        # Remove bare URLs (http/https)
        markdown_content = re.sub(r'https?://[^\s\)\]\n]+', '', markdown_content)
        
        # Remove reference-style links: [text][ref] -> text
        markdown_content = re.sub(r'\[([^\]]*)\]\[[^\]]*\]', r'\1', markdown_content)
        
        # Remove reference definitions: [ref]: url
        markdown_content = re.sub(r'^\s*\[[^\]]+\]:\s*.*$', '', markdown_content, flags=re.MULTILINE)
        
        # Clean up multiple whitespaces and empty lines
        markdown_content = re.sub(r'\n\s*\n\s*\n', '\n\n', markdown_content)
        markdown_content = re.sub(r' +', ' ', markdown_content)
        
        return markdown_content.strip()

    def create_chunk_prompt(self, product_chunk: List[Dict[str, Any]]) -> str:
        """Create a prompt with clean, link-free markdown content from a chunk of products."""
        markdown_contents = []
        
        for i, product in enumerate(product_chunk, 1):
            url = product.get('url', 'Unknown URL')
            markdown = product.get('markdown_content', '')
            
            # IMPORTANT: Remove all links from markdown content before processing
            markdown = self.remove_links_from_markdown(markdown)
            
            # Add very clear separator with product number and URL
            separator = "=" * 80
            chunk_header = f"\n{separator}\n=== PRODUCT {i} - URL: {url} ===\n{separator}\n"
            chunk_footer = f"\n{separator}\n=== END OF PRODUCT {i} ===\n{separator}\n"
            
            # Since content is now clean and link-free, we can be more generous with content length
            # The enhanced scraper provides higher quality, more focused content
            if len(markdown) > 12000:  # Increased from 8k to 12k due to better content quality
                # Take the first part (which usually has the main product info)
                markdown = markdown[:12000] + "\n... [Content truncated - main product information preserved] ..."
            
            # Add content quality indicator for Gemini
            quality_note = "\n[NOTE: This content has been DOUBLE-PROCESSED - all links removed, navigation filtered, advanced crawling, pure product content optimized]\n"
            
            product_content = chunk_header + quality_note + markdown + chunk_footer
            markdown_contents.append(product_content)
        
        combined_content = "\n".join(markdown_contents)
        
        # Use the enhanced template with dynamic placeholders
        return self.prompt_template.format(
            num_products=len(product_chunk),
            markdown_content=combined_content
        )

    def process_chunk_with_gemini(self, chunk: List[Dict[str, Any]], chunk_num: int) -> List[Dict[str, Any]]:
        """Process a chunk of products with Gemini API."""
        try:
            print(f"🔄 Processing chunk {chunk_num} ({len(chunk)} products)...")
            
            # Create prompt with markdown content
            prompt = self.create_chunk_prompt(chunk)
            
            # Send to Gemini
            response = self.model.generate_content(prompt)
            
            if not response.text:
                print(f"❌ Empty response from Gemini for chunk {chunk_num}")
                return []
            
            # Try to parse JSON response
            try:
                # Clean the response text
                response_text = response.text.strip()
                
                # Remove any markdown formatting
                lines = response_text.split('\n')
                cleaned_lines = []
                for line in lines:
                    if not line.strip().startswith('```'):
                        cleaned_lines.append(line)
                
                response_text = '\n'.join(cleaned_lines).strip()
                
                # Try to find and extract JSON
                import re
                
                # First try to find a JSON array
                array_pattern = r'\[[\s\S]*?\]'
                array_match = re.search(array_pattern, response_text)
                
                if array_match:
                    json_text = array_match.group(0)
                    try:
                        extracted_data = json.loads(json_text)
                        if isinstance(extracted_data, list):
                            # Validate: should have exactly the same number of products as input URLs
                            expected_count = len(chunk)
                            actual_count = len(extracted_data)
                            
                            if actual_count == expected_count:
                                # Verify URLs match (add the actual URLs to products if missing)
                                for i, (product_data, original_product) in enumerate(zip(extracted_data, chunk)):
                                    if not product_data.get('Web URL') or product_data.get('Web URL') == 'Unknown URL':
                                        product_data['Web URL'] = original_product.get('url', 'Unknown URL')
                                
                                print(f"✅ Successfully processed chunk {chunk_num}: {actual_count} products extracted (correct count)")
                                return extracted_data
                            elif actual_count > expected_count:
                                # Take only the first N products if too many
                                trimmed_data = extracted_data[:expected_count]
                                # Fix URLs for trimmed data
                                for i, (product_data, original_product) in enumerate(zip(trimmed_data, chunk)):
                                    if not product_data.get('Web URL') or product_data.get('Web URL') == 'Unknown URL':
                                        product_data['Web URL'] = original_product.get('url', 'Unknown URL')
                                
                                print(f"⚠️ Chunk {chunk_num}: Got {actual_count} products, trimmed to {len(trimmed_data)} (expected {expected_count})")
                                return trimmed_data
                            else:
                                # Less products than expected - still return what we got but log warning
                                for i, product_data in enumerate(extracted_data):
                                    if i < len(chunk):
                                        if not product_data.get('Web URL') or product_data.get('Web URL') == 'Unknown URL':
                                            product_data['Web URL'] = chunk[i].get('url', 'Unknown URL')
                                
                                print(f"⚠️ Chunk {chunk_num}: Expected {expected_count} products, got {actual_count}")
                                return extracted_data
                    except:
                        pass
                
                # If no array found, try to find JSON objects
                object_pattern = r'\{[\s\S]*?\}'
                object_matches = re.findall(object_pattern, response_text)
                
                if object_matches:
                    extracted_data = []
                    for obj_text in object_matches:
                        try:
                            obj = json.loads(obj_text)
                            extracted_data.append(obj)
                        except:
                            continue
                    
                    if extracted_data:
                        print(f"✅ Successfully processed chunk {chunk_num}: {len(extracted_data)} products extracted")
                        return extracted_data
                
                # If still no success, try the entire response as JSON
                try:
                    extracted_data = json.loads(response_text)
                    if isinstance(extracted_data, dict):
                        extracted_data = [extracted_data]
                    elif isinstance(extracted_data, list):
                        pass
                    else:
                        extracted_data = []
                    
                    if extracted_data:
                        print(f"✅ Successfully processed chunk {chunk_num}: {len(extracted_data)} products extracted")
                        return extracted_data
                except:
                    pass
                
                print(f"❌ No valid JSON found in response for chunk {chunk_num}")
                print(f"Response preview: {response_text[:500]}...")
                return []
                
            except Exception as e:
                print(f"❌ Error parsing response for chunk {chunk_num}: {str(e)}")
                return []
                
        except Exception as e:
            print(f"❌ Error processing chunk {chunk_num}: {str(e)}")
            return []

    def process_all_products(self, input_file: str, output_file: str):
        """Process all products from rakuten.json and save to rakuten_final.json."""
        print("🚀 Starting Rakuten product processing with Gemini...")
        
        # Load products
        products = self.load_rakuten_data(input_file)
        if not products:
            print("❌ No products to process")
            return
        
        # Split into chunks
        chunks = [products[i:i + self.chunk_size] for i in range(0, len(products), self.chunk_size)]
        print(f"📦 Split {len(products)} products into {len(chunks)} chunks of {self.chunk_size} each")
        
        # Process each chunk
        all_extracted_data = []
        successful_chunks = 0
        
        for i, chunk in enumerate(chunks, 1):
            try:
                extracted_data = self.process_chunk_with_gemini(chunk, i)
                
                if extracted_data:
                    all_extracted_data.extend(extracted_data)
                    successful_chunks += 1
                
                # Add delay between requests to respect API limits
                if i < len(chunks):  # Don't delay after the last chunk
                    print(f"⏳ Waiting {self.delay_between_requests} seconds before next chunk...")
                    time.sleep(self.delay_between_requests)
                    
            except Exception as e:
                print(f"❌ Failed to process chunk {i}: {str(e)}")
                continue
        
        # Save results
        if all_extracted_data:
            output_data = {
                "metadata": {
                    "total_products_processed": len(products),
                    "total_chunks": len(chunks),
                    "successful_chunks": successful_chunks,
                    "extracted_products": len(all_extracted_data),
                    "processing_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "chunk_size": self.chunk_size
                },
                "products": all_extracted_data
            }
            
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)
                
                print(f"✅ Successfully saved {len(all_extracted_data)} extracted products to {output_file}")
                print(f"📊 Processing Summary:")
                print(f"   - Total products: {len(products)}")
                print(f"   - Successful chunks: {successful_chunks}/{len(chunks)}")
                print(f"   - Extracted products: {len(all_extracted_data)}")
                
            except Exception as e:
                print(f"❌ Error saving results: {str(e)}")
        else:
            print("❌ No data was successfully extracted")

def main():
    """Main function to run the processing."""
    input_file = "rakuten.json"
    output_file = "rakuten_final.json"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"❌ Input file {input_file} not found")
        return
    
    # Initialize processor and run
    try:
        processor = RakutenGeminiProcessor()
        processor.process_all_products(input_file, output_file)
    except Exception as e:
        print(f"❌ Error initializing processor: {str(e)}")

if __name__ == "__main__":
    main()

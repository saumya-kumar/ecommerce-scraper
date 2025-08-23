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
        
        # Use Gemini 2.5 Flash-Lite as specified in .env
        self.model = genai.GenerativeModel('gemini-2.5-flash-lite')
        
        # Processing configuration
        self.chunk_size = 5  # Smaller chunks for better clarity and accuracy
        self.delay_between_requests = 30  # 30 second delay between API calls
        
        # Prompt template
        self.prompt_template = """You are an expert e-commerce data extractor. You will be given {num_products} separate product pages from Rakuten.

CRITICAL INSTRUCTIONS:
1. Each product page is clearly marked with "=== PRODUCT X - URL: [url] ===" 
2. Extract information ONLY from the content between these markers for each product
3. Return EXACTLY {num_products} products in the JSON array - one for each URL provided
4. Match each product to its specific URL
5. Do NOT mix information between different products

TASK: Extract product information from each product page separately.

{markdown_content}

For each product page, extract these fields (extract what you can find, use null only if truly not available):
- Product Name: The main product title shown on that specific page [REQUIRED - must extract]
- Product Description: Main product description from that page only [IMPORTANT - try to extract]
- Price: The price shown on that specific page [IMPORTANT - look for ¥ or price info]
- Release Date: Product release date if mentioned [can be null]
- Volume/Size: Product size/volume information [look for ml, g, oz etc.]
- Country of Origin: Manufacturing country [can be null]
- Brand Name: Brand of the product [IMPORTANT - usually in title or header]
- Brand Description: Description of the brand [can be null]
- Full Ingredient List: Complete ingredients if available [extract if found]
- New Feature Promotion: Any promotional features mentioned [can be null]
- Marketing Materials: Marketing text or claims [can be null]
- Packaging Information: Package details [can be null]
- Web URL: Use the EXACT URL provided in the marker for that product [REQUIRED]

IMPORTANT OUTPUT FORMAT:
Return EXACTLY {num_products} products as a JSON array. Each product corresponds to one URL in order:

[
  {{
    "Product Name": "name from product 1",
    "Product Description": "description from product 1", 
    "Price": "price from product 1",
    "Release Date": null,
    "Volume/Size": "size from product 1",
    "Country of Origin": "country from product 1",
    "Brand Name": "brand from product 1",
    "Brand Description": null,
    "Full Ingredient List": "ingredients from product 1",
    "New Feature Promotion": null,
    "Marketing Materials": null,
    "Packaging Information": null,
    "Web URL": "exact_url_from_product_1_marker"
  }},
  {{
    "Product Name": "name from product 2",
    ...
  }}
]

VALIDATION: The output array must have exactly {num_products} elements, matching the {num_products} product pages provided."""

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

    def create_chunk_prompt(self, product_chunk: List[Dict[str, Any]]) -> str:
        """Create a prompt with markdown content from a chunk of products."""
        markdown_contents = []
        
        for i, product in enumerate(product_chunk, 1):
            url = product.get('url', 'Unknown URL')
            markdown = product.get('markdown_content', '')
            
            # Add very clear separator with product number and URL
            separator = "=" * 80
            chunk_header = f"\n{separator}\n=== PRODUCT {i} - URL: {url} ===\n{separator}\n"
            chunk_footer = f"\n{separator}\n=== END OF PRODUCT {i} ===\n{separator}\n"
            
            # Limit markdown content to avoid overwhelming the model
            if len(markdown) > 8000:  # Limit each product to ~8k characters
                markdown = markdown[:8000] + "\n... [Content truncated for brevity] ..."
            
            product_content = chunk_header + markdown + chunk_footer
            markdown_contents.append(product_content)
        
        combined_content = "\n".join(markdown_contents)
        
        # Use the template with dynamic placeholders
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

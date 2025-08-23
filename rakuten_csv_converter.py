import json
import csv
import os
from typing import List, Dict, Any

class RakutenCSVConverter:
    def __init__(self):
        """Initialize the CSV converter."""
        self.input_file = "rakuten_final.json"
        self.output_file = "rakuten.csv"
        
        # Define the CSV columns in the desired order
        self.csv_columns = [
            "Product Name",
            "Product Description", 
            "Price",
            "Release Date",
            "Volume/Size",
            "Country of Origin",
            "Brand Name",
            "Brand Description",
            "Full Ingredient List",
            "New Feature Promotion",
            "Marketing Materials",
            "Packaging Information",
            "Web URL"
        ]

    def load_json_data(self) -> List[Dict[str, Any]]:
        """Load product data from rakuten_final.json."""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
                products = data.get('products', [])
                print(f"✅ Loaded {len(products)} products from {self.input_file}")
                return products
        except FileNotFoundError:
            print(f"❌ File {self.input_file} not found")
            return []
        except json.JSONDecodeError as e:
            print(f"❌ Error decoding JSON: {str(e)}")
            return []
        except Exception as e:
            print(f"❌ Error loading file: {str(e)}")
            return []

    def clean_data_for_csv(self, value: Any) -> str:
        """Clean and format data for CSV output."""
        if value is None:
            return ""
        
        # Handle lists (like Marketing Materials)
        if isinstance(value, list):
            # Join list items with semicolon for CSV compatibility
            return "; ".join(str(item) for item in value if item)
        
        # Handle dictionaries (convert to string representation)
        if isinstance(value, dict):
            return str(value)
        
        # Convert to string and clean up
        value_str = str(value).strip()
        
        # Remove excessive whitespace and newlines for CSV
        value_str = " ".join(value_str.split())
        
        return value_str

    def count_null_values(self, product: Dict[str, Any]) -> int:
        """Count the number of null/empty values in a product."""
        null_count = 0
        for column in self.csv_columns:
            value = product.get(column)
            if value is None or value == "" or value == []:
                null_count += 1
        return null_count

    def convert_to_csv(self):
        """Convert rakuten_final.json to rakuten.csv."""
        print("🚀 Starting JSON to CSV conversion...")
        
        # Load JSON data
        products = self.load_json_data()
        if not products:
            print("❌ No products to convert")
            return
        
        # Convert to CSV
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.csv_columns)
                
                # Write header
                writer.writeheader()
                
                # Process each product
                processed_count = 0
                skipped_count = 0
                
                for product in products:
                    # Must have Product Name and Web URL at minimum
                    if not product.get('Product Name') or not product.get('Web URL'):
                        skipped_count += 1
                        print(f"⏭️  Skipping product missing name/URL: {product.get('Web URL', 'Unknown URL')[:50]}...")
                        continue
                    
                    # Clean and prepare row data
                    row_data = {}
                    for column in self.csv_columns:
                        raw_value = product.get(column)
                        cleaned_value = self.clean_data_for_csv(raw_value)
                        row_data[column] = cleaned_value
                    
                    # Write row to CSV
                    writer.writerow(row_data)
                    processed_count += 1
                
                print(f"✅ Successfully converted {processed_count} products to {self.output_file}")
                print(f"⏭️  Skipped {skipped_count} products missing critical fields (name/URL)")
                print(f"📁 CSV file saved with {len(self.csv_columns)} columns")
                
                # Show column summary
                print(f"📊 CSV Columns:")
                for i, column in enumerate(self.csv_columns, 1):
                    print(f"   {i:2d}. {column}")
                
        except Exception as e:
            print(f"❌ Error writing CSV file: {str(e)}")

    def show_csv_preview(self, rows: int = 3):
        """Show a preview of the generated CSV file."""
        try:
            if not os.path.exists(self.output_file):
                print(f"❌ CSV file {self.output_file} not found")
                return
            
            print(f"\n📋 Preview of {self.output_file} (first {rows} rows):")
            print("-" * 80)
            
            with open(self.output_file, 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile)
                
                # Show header
                header = next(reader, None)
                if header:
                    print("HEADERS:")
                    for i, col in enumerate(header, 1):
                        print(f"  {i:2d}. {col}")
                    print()
                
                # Show first few rows
                for i, row in enumerate(reader):
                    if i >= rows:
                        break
                    print(f"ROW {i+1}:")
                    for j, cell in enumerate(row):
                        if j < len(header):
                            # Truncate long values for preview
                            display_value = cell[:100] + "..." if len(cell) > 100 else cell
                            print(f"  {header[j]}: {display_value}")
                    print()
                
                # Count total rows
                with open(self.output_file, 'r', encoding='utf-8-sig') as f:
                    total_rows = sum(1 for line in f) - 1  # Subtract header
                    print(f"📊 Total data rows: {total_rows}")
                
        except Exception as e:
            print(f"❌ Error reading CSV preview: {str(e)}")

def main():
    """Main function to run the CSV conversion."""
    print("🔄 Rakuten JSON to CSV Converter")
    print("=" * 50)
    
    # Initialize converter
    converter = RakutenCSVConverter()
    
    # Check if input file exists
    if not os.path.exists(converter.input_file):
        print(f"❌ Input file {converter.input_file} not found")
        print("   Please make sure rakuten_final.json exists in the current directory")
        return
    
    # Convert to CSV
    converter.convert_to_csv()
    
    # Show preview
    converter.show_csv_preview()
    
    print("\n✅ CSV conversion completed successfully!")

if __name__ == "__main__":
    main()

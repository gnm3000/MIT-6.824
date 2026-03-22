import faker
import faker_commerce
## here we need to generate a CSV file data with:
"""
- id 
- product name
- price
- quantity
- customer name
- datetime of purchase
- store_id (can be 1,2,3,4 or 5) we have 5 stores
- payment method (can be cash, credit card, debit card, mobile payment)
"""

def generate_data(num_rows):
    fake = faker.Faker()
    fake.add_provider(faker_commerce.Provider)
    data = []
    for i in range(num_rows):
        row = {
            'id': i + 1,
            'product_name': fake.ecommerce_name(),
            'price': round(fake.random_number(digits=5) / 100, 2),
            'quantity': fake.random_int(min=1, max=10),
            'customer_name': fake.name(),
            'datetime_of_purchase': fake.date_time_this_year().isoformat(),
            'store_id': fake.random_int(min=1, max=5),
            'payment_method': fake.random_element(elements=('cash', 'credit card', 'debit card', 'mobile payment'))
        }
        data.append(row)
    return data

if __name__ == "__main__":
    num_rows = 1000  # specify how many rows you want to generate
    generated_data = generate_data(num_rows)
    
    # Now you can write this data to a CSV file
    
    import csv
    with open('generated_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['id', 'product_name', 'price', 'quantity', 'customer_name', 'datetime_of_purchase', 'store_id', 'payment_method']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in generated_data:
            writer.writerow(row)
import random
import string
import pandas as pd

def generate_random_string(length: int) -> str:
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_random_email() -> str:
    domains = ["example.com", "test.com", "sample.org"]
    return f"{generate_random_string(5)}@{random.choice(domains)}"

def generate_random_phone_number() -> str:
    return ''.join(random.choices(string.digits, k=10))

def generate_user_records(num_records: int) -> pd.DataFrame:
    records = []
    for _ in range(num_records):
        first_name = generate_random_string(6)
        last_name = generate_random_string(8)
        email = generate_random_email()
        phone_number = generate_random_phone_number()
        records.append((first_name, last_name, email, phone_number))
    
    return pd.DataFrame(records, columns=["first_name", "last_name", "email", "phone_number"])

def main():
    num_records = 10000
    user_records_df = generate_user_records(num_records)
    output_file = "/home/bhavesh/Talentica/2_data_lake/debezium-mysql-sync/input_data.xlsx"
    user_records_df.to_excel(output_file, index=False)
    print(f"Generated {num_records} user records and saved to {output_file}")

if __name__ == "__main__":
    main()
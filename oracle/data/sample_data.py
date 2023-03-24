import random
from uuid import uuid4
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Generate 4 countries and their currencies
country_currencies = [
    ("US", "United States", "USD", "US Dollar", "$"),
    ("UK", "United Kingdom", "GBP", "British Pound", "£"),
    ("CA", "Canada", "CAD", "Canadian Dollar", "C$"),
    ("AU", "Australia", "AUD", "Australian Dollar", "A$")
]

# Generate 50 clients
clients = []
for i in range(50):
    client_id = i + 1
    country = random.choice(country_currencies)
    clients.append((client_id, fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(),
                    fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
                    fake.address().replace("\n", ", "), country[0], country[2]))

# Generate 60 cards
cards = []
for i in range(60):
    card_id = i + 1
    client_id = i % 50 + 1
    cards.append((card_id, client_id, fake.credit_card_number(card_type="visa"), fake.credit_card_expire(),
                  fake.credit_card_security_code(card_type="visa"), 'ACTIVE'))

# Generate 55 accounts
accounts = []
for i in range(55):
    account_id = i + 1
    client_id = i % 50 + 1
    currency_code = clients[client_id - 1][-1]
    accounts.append((account_id, client_id, currency_code, random.uniform(0, 10000), 'ACTIVE'))


# Generate 1000 transactions
transactions = []
start_date = datetime.strptime("2023-01-01", "%Y-%m-%d")
end_date = datetime.now().date()
transaction_types = ['restaurant', 'grocery', 'atm', 'online', 'salary']
salary_dates = {}  # Store salary transaction dates for each client

for i in range(1000):
    transaction_id = str(uuid4())
    card_id = i % 60 + 1
    account_id = i % 55 + 1
    city = fake.city()
    transaction_type = random.choice(transaction_types)

    if transaction_type == 'salary':
        client_id = (card_id - 1) % 50 + 1
        currency_code = clients[client_id - 1][-1]
        amount = round(random.uniform(500, 5000), 2)
        description = f"Received salary of ${amount} {currency_code}"

        # Ensure salary transaction happens only once a month for a client
        if client_id not in salary_dates:
            salary_dates[client_id] = fake.date_between(start_date=start_date, end_date=end_date)
            salary_date = salary_dates[client_id].replace(day=random.randint(1, 5))
        else:
            current_month = salary_dates[client_id].month
            next_month = current_month + 1 if current_month < 12 else 1
            next_year = salary_dates[client_id].year + 1 if next_month == 1 else salary_dates[client_id].year
            proposed_salary_date = salary_dates[client_id].replace(month=next_month, year=next_year, day=random.randint(1, 5))

            # Check if proposed_salary_date is at the beginning of the month and strictly less than end_date
            if proposed_salary_date.day <= 5 and proposed_salary_date < end_date:
                salary_dates[client_id] = proposed_salary_date
            else:
                # If proposed_salary_date is in the future, adjust it to the current month and year
                proposed_salary_date = proposed_salary_date.replace(month=end_date.month, year=end_date.year)

                if proposed_salary_date > end_date:
                    # If the adjusted date is still in the future, go back one month
                    prev_month = end_date.month - 1 if end_date.month > 1 else 12
                    prev_year = end_date.year if end_date.month > 1 else end_date.year - 1
                    proposed_salary_date = proposed_salary_date.replace(month=prev_month, year=prev_year)

                salary_dates[client_id] = proposed_salary_date

            salary_date = salary_dates[client_id]

        transaction_date = salary_date.strftime('%Y-%m-%d %I:%M %p')

    else:

        if transaction_type == 'restaurant':
            amount = round(random.uniform(-150, -15), 2)
            description = f"Spent ${abs(amount)} at {fake.company()} in {city}"

        elif transaction_type == 'grocery':
            amount = round(random.uniform(-200, -30), 2)
            description = f"Spent ${abs(amount)} at {fake.company()} in {city}"

        elif transaction_type == 'atm':
            amount = -random.choice([10, 20, 50, 100, 200])
            description = f"Withdrew ${abs(amount)} from ATM in {city}"

        elif transaction_type == 'online':
            amount = round(random.uniform(-300, -5), 2)
            description = f"Spent ${abs(amount)} on {fake.domain_name()}"

        elif transaction_type == 'salary':
            client_id = (card_id - 1) % 50 + 1
            currency_code = clients[client_id - 1][-1]
            amount = round(random.uniform(500, 5000), 2)
            description = f"Received salary of ${amount} {currency_code}"

        transaction_date = fake.date_time_between(start_date=start_date, end_date=end_date).strftime('%Y-%m-%d %I:%M %p')
    
    transactions.append((transaction_id, card_id, account_id, amount, f"{description} on {transaction_date}.", transaction_date))

with open("sample_data.sql", "w") as f:
    f.write("INSERT ALL\n")
    for country in country_currencies:
        f.write(f"  INTO country (country_code, country_name) VALUES ('{country[0]}', '{country[1]}')\n")
        
    for currency in country_currencies:
        f.write(f"  INTO currency (currency_code, currency_name, currency_symbol) VALUES ('{currency[2]}', '{currency[3]}', '{currency[4]}')\n")
        
    for client in clients:
        f.write(f"  INTO client (client_id, first_name, last_name, email, phone_number, date_of_birth, address, country_code) VALUES ({client[0]}, '{client[1]}', '{client[2]}', '{client[3]}', '{client[4]}', TO_DATE('{client[5]}', 'YYYY-MM-DD'), '{client[6]}', '{client[7]}')\n")
        
    for card in cards:
        f.write(f"  INTO card (card_id, client_id, card_number, expiration_date, cvv, status) VALUES ({card[0]}, {card[1]}, '{card[2]}', TO_DATE('{card[3]}', 'MM/YYYY'), '{card[4]}', '{card[5]}')\n")
        
    for account in accounts:
        f.write(f"  INTO account (account_id, client_id, currency_code, balance, status) VALUES ({account[0]}, {account[1]}, '{account[2]}', {account[3]}, '{account[4]}')\n")

    for transaction in transactions:
        f.write(f"  INTO transaction (transaction_id, card_id, account_id, amount, description, transaction_ts) VALUES ('{transaction[0]}', {transaction[1]}, {transaction[2]}, {transaction[3]}, '{transaction[4]}', TO_DATE('{transaction[5]}', 'YYYY-MM-DD HH:MI AM'))\n")

    f.write("SELECT * FROM dual;\n")

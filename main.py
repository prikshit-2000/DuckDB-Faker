import faker
import duckdb
from fastapi import FastAPI
import json

app = FastAPI()


fake = faker.Faker()
def connect_db():
    con = duckdb.connect(database='fake_customers.db')
    return con


def create_table(con):
    con.execute("""
            Create table  main.customers
            (
                customer_id integer primary key,
                first_name varchar not null,
                last_name varchar not null,
                email varchar not null,
                address varchar not null,
                phone_number varchar not null,
                date_of_birth date not null,
                date_created date not null default current_date,
                date_updated date not null default current_date,
            )
            """)
    k = con.execute("desc customers").df()
    con.close()
    
def populate_data_with_fakes(con):
    customers = []
    for i in range(50):

        customers.append([
        i+1,
        fake.first_name(),
        fake.last_name(),
        fake.address(),
        fake.email(),
        fake.phone_number(),
        fake.date_of_birth(minimum_age=18, maximum_age=72)
        ]
        )
    con.executemany(
    """
    INSERT INTO main.customers
    (customer_id, first_name, last_name,email,address,phone_number,date_of_birth)
     values (?,?,?,?,?,?,?)
    """,
    customers
    )
    
    
def query_customers(con):
    query = """Select * from main.customers"""
    j = con.execute(query).df()
    print(j)
    

@app.get('/customers')
def get_customers():
    con = connect_db()
    customers = con.execute('select * from main.customers').df().to_dict(orient='records')
    
    
    con.close()
    return customers
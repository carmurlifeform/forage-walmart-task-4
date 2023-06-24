import csv
import sqlite3
import pandas as pd

db = sqlite3.connect('shipment_database.db')
cursor = db.cursor()

cursor.execute("DELETE FROM shipment")
cursor.execute("DELETE FROM product")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

file = open("data/shipping_data_0.csv")
contents = csv.reader(file)
next(csv.reader(file), None)

def prod_name_to_id(x):
    cursor.execute("SELECT id FROM product WHERE name=?", (x,))
    prow = cursor.fetchone()
    if prow is None:
        cursor.execute("INSERT INTO product (name) values(?)", (x,))
        prow = (cursor.lastrowid,)
    return prow[0]


for entry in contents:
    try:
        record = (prod_name_to_id(entry[2]), int(entry[4]), entry[0], entry[1])
        cursor.execute("INSERT INTO shipment (product_id,quantity,origin,destination) values(?,?,?,?)", record)
    except csv.Error as e:
        print("err")
db.commit()
dataset1 = pd.read_csv("data/shipping_data_1.csv")
dataset2 = pd.read_csv("data/shipping_data_2.csv")

dataset1["product_quantity"] = dataset1.groupby("shipment_identifier")["product"].transform(
    lambda x: x.map(x.value_counts())
)
dataset3 = pd.merge(dataset1.drop_duplicates(), dataset2, on='shipment_identifier')

dataset3['product'] = dataset3['product'].map(lambda x: prod_name_to_id(x))

dataset3.to_sql('insertion',db)
cursor.execute("INSERT INTO shipment (product_id,quantity,origin,destination) SELECT product,product_quantity,origin_warehouse,destination_store FROM insertion")
cursor.execute("DROP TABLE INSERTION")
db.commit()

db.close()


import requests
import json
import os
import sqlite3
import datetime
import time

token = '1567153043b92ef2b4e43f88db0fdb99f760653e'
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        
    
def Create_Database():
    if not os.path.exists("Database"):
        os.mkdir("Database");
    connection = sqlite3.connect('Database/database.db')
    cursor = connection.cursor()

    # Создаем таблицы
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS RetailStores (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
    );''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    cost TEXT NOT NULL,
    countinstock INTEGER NOT NULL
    );''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS RetailDemands (
    id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    count INTEGER NOT NULL,
    retailstore_id TEXT NOT NULL,
    datetime TEXT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES Products (id),
    FOREIGN KEY (retailstore_id) REFERENCES RetailStores (id)
    );''')         
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS RetailSalesReturns (
    id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    count INTEGER NOT NULL,
    retailstore_id TEXT NOT NULL,
    datetime TEXT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES Products (id),
    FOREIGN KEY (retailstore_id) REFERENCES RetailStores (id)
    );''')
    
    # Сохраняем изменения и закрываем соединение
    connection.commit()
    connection.close()    

def Get_Json(Link, Name):
    response = requests.get(Link, headers=headers)
    result = response.json()
    with open("Input/{0}.json".format(Name), "w+") as write_file:
        json.dump(result, write_file, ensure_ascii = False, indent='\t')
        
def Get_DATA():
    if not os.path.exists("Input"):
        os.mkdir("Input");
    print("OG")
    #Получение точек продаж
    Get_Json("https://api.moysklad.ru/api/remap/1.2/entity/retailstore", "retailstore")
    #Получение Продаж
    Get_Json("https://api.moysklad.ru/api/remap/1.2/entity/retaildemand", "retaildemand")
    #Получение Возвратов
    Get_Json("https://api.moysklad.ru/api/remap/1.2/entity/retailsalesreturn", "retailsalesreturn")
    #Получение Товаров
    Get_Json("https://api.moysklad.ru/api/remap/1.2/entity/product", "product") 
    Get_Json("https://api.moysklad.ru/api/remap/1.2/report/stock/all/current", "stockallcurrent")    
    
def Put_DATA():
    
    #Внесение торговых точек в базу
    connection = sqlite3.connect('Database/database.db')
    cursor = connection.cursor()
    with open("Input/retailstore.json", "r") as read_file:
        data = json.load(read_file)
    for txt in data['rows']:
        print(txt["id"] + ' ' + txt["name"])
        cursor.execute('INSERT INTO RetailStores (id, name) VALUES (?, ?)', (txt["id"], txt["name"]))
    connection.commit()
    connection.close()
    
    #Внесение Товаров в базу
    connection = sqlite3.connect('Database/database.db')
    cursor = connection.cursor()
    with open("Input/product.json", "r") as read_file:
        data = json.load(read_file)
    with open("Input/stockallcurrent.json", "r") as read_file:
        data1 = json.load(read_file)
    for txt in data['rows']:
        for txt1 in data1:
             if txt1['assortmentId'] == txt['id']:
                 value = str(txt["salePrices"][0]["value"])
                 cursor.execute('INSERT INTO Products (id, name, cost, countinstock) VALUES (?, ?, ?, ?)', (txt["id"], txt["name"], value, txt1["stock"]))
    connection.commit()
    connection.close()
    
    #Внесение продаж в базу
    connection = sqlite3.connect('Database/database.db')
    cursor = connection.cursor()
    with open("Input/retaildemand.json", "r") as read_file:
        data = json.load(read_file)
    for txt in data['rows']:
        response = requests.get(txt["retailStore"]["meta"]["href"], headers=headers)
        result = response.json()
        retailstore_id = result["id"]

        response = requests.get(txt["positions"]["meta"]["href"], headers=headers)
        result = response.json()
        count = result["rows"][0]["quantity"]

        response = requests.get(txt["positions"]["meta"]["href"], headers=headers)
        result = response.json()
        response = requests.get(result['rows'][0]["assortment"]["meta"]["href"], headers=headers)
        result = response.json()
        product_id = result["id"]
        
        cursor.execute('INSERT INTO RetailDemands (id, product_id, count, retailstore_id, datetime) VALUES (?, ?, ?, ?, ?)', (txt["id"], product_id, count, retailstore_id, txt["moment"]))
    connection.commit()
    connection.close()
    
    #Внесение возвратов в базу
    connection = sqlite3.connect('Database/database.db')
    cursor = connection.cursor()
    with open("Input/retailsalesreturn.json", "r") as read_file:
        data = json.load(read_file)
    for txt in data['rows']:
        response = requests.get(txt["retailStore"]["meta"]["href"], headers=headers)
        result = response.json()
        retailstore_id = result["id"]

        response = requests.get(txt["positions"]["meta"]["href"], headers=headers)
        result = response.json()
        count = result["rows"][0]["quantity"]

        response = requests.get(txt["positions"]["meta"]["href"], headers=headers)
        result = response.json()
        response = requests.get(result['rows'][0]["assortment"]["meta"]["href"], headers=headers)
        result = response.json()
        product_id = result["id"]
        cursor.execute('INSERT INTO RetailSalesReturns (id, product_id, count, retailstore_id, datetime) VALUES (?, ?, ?, ?, ?)', (txt["id"], product_id, count, retailstore_id, txt["moment"]))
    connection.commit()
    connection.close()



def Analyze_Orders(retailstoreid, productid, daystep):
    sum = 0
    date = datetime.datetime.now()
    litedate = datetime.datetime(date.year, date.month, date.day, hour=0, minute=0, second=0, microsecond=0)
    connection = sqlite3.connect('Database/database.db')
    cursor = connection.cursor()
    #Считаем продажи
    cursor.execute("SELECT * FROM RetailDemands WHERE retailstore_id =? and product_id=?", (retailstoreid, productid))
    rows = cursor.fetchall()
    result_list = [list(row) for row in rows]
    for txt in result_list:
        dat_str = txt[4].split(" ")[0]
        dat_obj = datetime.datetime.strptime(dat_str, '%Y-%m-%d')
        if dat_obj == (litedate + datetime.timedelta(days=daystep)):
            sum+=int(txt[2])
        #print(dat_obj)
    #print(result_list)
    
    
    #Считаем возвраты
    cursor.execute("SELECT * FROM RetailSalesReturns WHERE retailstore_id =? and product_id=?", (retailstoreid, productid))
    rows = cursor.fetchall()
    result_list = [list(row) for row in rows]
    for txt in result_list:
        dat_str = txt[4].split(" ")[0]
        dat_obj = datetime.datetime.strptime(dat_str, '%Y-%m-%d')
        if dat_obj == (litedate + datetime.timedelta(days=daystep)):
            sum-=int(txt[2])
        #print(dat_obj)
    #print(result_list)
    
    #print(sum)
    connection.close()
    return sum


    
def GiveOut_Orders(data):
    if not os.path.exists("Output"):
        os.mkdir("Output");
    with open("Output/retailsalesreturn.json", "w") as write_file:
        data = json.load(write_file)
        json.dump(data, write_file, ensure_ascii = False, indent='\t')
    

IsWorking = True
while IsWorking:
    offset = datetime.timedelta(hours=3)
    tz = datetime.timezone(offset, name='МСК')
    today = datetime.datetime.now(tz=tz)
    timenow = [today.hour, today.minute]
    weekday = today.isoweekday()
    #print(weekday)
    if timenow != [1, 0]:
        #вторник-суббота
        if weekday > 1 and weekday < 7:
            daystep = -1
            result_list = []
            connection = sqlite3.connect('Database/database.db')
            connection.row_factory = lambda cursor, row: row[0]
            cursor = connection.cursor()
            cursor.execute("SELECT id FROM RetailStores")
            rows = cursor.fetchall()
            retailstores = list(rows)
            cursor.execute("SELECT id FROM Products")
            rows = cursor.fetchall()
            products = list(rows)
            
            for i in range(len(retailstores)):
                for j in range(len(products)):
                    #print(Analyze_Orders(retailstores[i], products[j], daystep))
                    #print(retailstores[i] +'   '+ products[j])
                    result_list.append([retailstores[i], products[j], Analyze_Orders(retailstores[i], products[j], daystep)])
            print(result_list)###
            connection.close()
        #понедельник
        elif weekday == 1:
            daystep = -2
            result_list = []
            connection = sqlite3.connect('Database/database.db')
            connection.row_factory = lambda cursor, row: row[0]
            cursor = connection.cursor()
            cursor.execute("SELECT id FROM RetailStores")
            rows = cursor.fetchall()
            retailstores = list(rows)
            cursor.execute("SELECT id FROM Products")
            rows = cursor.fetchall()
            products = list(rows)
            
            for i in range(len(retailstores)):
                for j in range(len(products)):
                    result_list.append([retailstores[i], products[j], Analyze_Orders(retailstores[i], products[j], daystep+1) + Analyze_Orders(retailstores[i], products[j], daystep)])
            print(result_list)###
            connection.close()
            
        time.sleep(60)
    
    time.sleep(1)

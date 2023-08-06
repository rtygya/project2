# Reety Gyawali 1001803756 
# Rakshav Patel – 10011941754

# pip3 install neo4j-driver

from neo4j import GraphDatabase, basic_auth

# Create nodes for each file
def create_vendor_item(vId, iId, Vprice):
    return f"CREATE (:VENDOR_ITEM{{iId: '{iId}', vId: '{vId}', Vprice: {float(Vprice)}}})"

def create_vendor(vId,Vname,Street,City,StateAb,ZipCode):
    return f"CREATE (:VENDOR{{vId: '{vId}', Vname: '{Vname}', Street: '{Street}', City: '{City}', StateAb: '{StateAb}', ZipCode: '{ZipCode}'}})"

def create_item(iId,Iname,Sprice):
    return f"CREATE (:ITEM{{iId: '{iId}', Iname: '{Iname}', Sprice: {float(Sprice)}}})"

def create_order(oId,sId,cId,Odate,Ddate,Amount):
    return f"CREATE (:ORDERS{{oId: '{oId}', sId: '{sId}', cId: '{cId}', Odate: '{Odate}', Ddate: '{Ddate}', Amount: {int(Amount)}}})"

def create_customer(cId,Cname,Street,City,StateAb,Zipcode):
    return f"CREATE (:CUSTOMER{{cId: '{cId}', Cname: '{Cname}', Street: '{Street}', City: '{City}', StateAb: '{StateAb}', Zipcode: '{Zipcode}'}})"

def create_store(sId,Sname,Street,City,StateAb,ZipCode,Sdate,Telno,URL):
    return f"CREATE(:STORE{{sId: '{sId}', Sname: '{Sname}', Street: '{Street}', City: '{City}', StateAb: '{StateAb}', ZipCode: '{ZipCode}', Sdate: '{Sdate}', Telno: '{Telno}', URL: '{URL}'}})"

def create_order_item(oId, iId, Icount):
    return f"CREATE(:ORDER_ITEM{{oId: '{oId}', iId: '{iId}', Icount: {int(Icount)}}})"

def create_oldprice(iId, Sprice, Sdate, Edate):
    return f"CREATE(:OLDPRICE{{iId: '{iId}', Sprice: {float(Sprice)}, Sdate: '{Sdate}', Edate: '{Edate}'}})"

def create_employee(sId,SSN,Sname,Street,City,StateAb,Zipcode,Etype,Bdate,Sdate,Edate,Level,Asalary,Agency,Hsalary,Institute,Itype):
    return f"CREATE(:EMPLOYEE{{sId: '{sId}', SSN: '{SSN}', Sname: '{Sname}', Street: '{Street}', City: '{City}', Bdate: '{Bdate}', StateAb: '{StateAb}', Zipcode: '{Zipcode}', Etype: '{Etype}', Sdate: '{Sdate}', Edate: '{Edate}', Level: '{Level}', Asalary: '{Asalary}', Agency: '{Agency}', Hsalary: {Hsalary}, Institute: '{Institute}', Itype: '{Itype}'}})"

def create_contract(vId,ctId,Sdate,Ctime,Cname):
    return f"CREATE(:CONTRACT{{vId: '{vId}', ctId: '{ctId}', Sdate: '{Sdate}', Ctime: '{Ctime}', Cname: '{Cname}'}})"

# Match query to return all nodes of table
def match_query(table_name):
    return f"MATCH (n:{table_name}) RETURN n"

# Sandbox info (can replace with your own)
driver = GraphDatabase.driver(
  "bolt://34.200.249.227:7687",
  auth=basic_auth("neo4j", "swings-airships-scopes"))

with driver.session(database="neo4j") as session:
    # Open each file and create nodes
    with open("Sprouts Data/VENDOR.csv") as v_file:
        next(v_file) # Skip header line
        for line in v_file:
            vId,Vname,Street,City,StateAb,ZipCode = line.strip().split(',')
            query = create_vendor(vId,Vname,Street,City,StateAb,ZipCode)
            # Run create query
            session.run(query)
            
    with open("Sprouts Data/VENDOR_ITEM.csv") as v_item_file:
        next(v_item_file)
        for line in v_item_file:
            vId,iId,Vprice = line.strip().split(',')
            query = create_vendor_item(vId, iId, Vprice)
            session.run(query)
    
    with open("Sprouts Data/ITEM.csv") as item_file:
        next(item_file)
        for line in item_file:
            iId,Iname,Sprice = line.strip().split(',')
            query = create_item(iId,Iname,Sprice)
            session.run(query)
    
    with open("Sprouts Data/ORDERS.csv") as orders_file:
        next(orders_file)
        for line in orders_file:
            oId,sId,cId,Odate,Ddate,Amount = line.strip().split(',')
            query = create_order(oId,sId,cId,Odate,Ddate,Amount)
            session.run(query)
     
    with open("Sprouts Data/CUSTOMER.csv") as customer_file:
        next(customer_file)
        for line in customer_file:
            cId,Cname,Street,City,StateAb,Zipcode = line.strip().split(',')
            query = create_customer(cId,Cname,Street,City,StateAb,Zipcode)
            session.run(query)
            
    with open("Sprouts Data/STORE.csv") as store_file:
        next(store_file)
        for line in store_file:
            sId,Sname,Street,City,StateAb,ZipCode,Sdate,Telno,URL = line.strip().split(',')
            query = create_store(sId,Sname,Street,City,StateAb,ZipCode,Sdate,Telno,URL)
            session.run(query) 

    with open("Sprouts Data/ORDER_ITEM.csv") as order_item_file:
        next(order_item_file)
        for line in order_item_file:
            oId, iId, Icount = line.strip().split(',')
            query = create_order_item(oId, iId, Icount)
            session.run(query)   

    with open("Sprouts Data/OLDPRICE.csv") as olderprice_file:
        next(olderprice_file)
        for line in olderprice_file:
            iId, Sprice, Sdate, Edate = line.strip().split(',')
            query = create_oldprice(iId, Sprice, Sdate, Edate)
            session.run(query)  

    with open("Sprouts Data/EMPLOYEE.csv") as employee_file:
        next(employee_file)
        for line in employee_file:
            sId,SSN,Sname,Street,City,StateAb,Zipcode,Etype,Bdate,Sdate,Edate,Level,Asalary,Agency,Hsalary,Institute,Itype = line.strip().split(',')
            query = create_employee(sId,SSN,Sname,Street,City,StateAb,Zipcode,Etype,Bdate,Sdate,Edate,Level,Asalary,Agency,Hsalary,Institute,Itype)
            session.run(query)

    with open("Sprouts Data/CONTRACT.csv") as contract_file:
        next(contract_file)
        for line in contract_file:
            vId,ctId,Sdate,Ctime,Cname = line.strip().split(',')
            query = create_contract(vId,ctId,Sdate,Ctime,Cname)
            session.run(query)
    
    # Print all VENDOR records
    print("VENDOR table:")
    m = match_query('VENDOR')
    result = session.run(m)
    for record in result:
        print(f"vId: {record['n']['vId']}, Vname: {record['n']['Vname']}, Street: {record['n']['Street']}, City: {record['n']['City']}, StateAb: {record['n']['StateAb']}, ZipCode: {record['n']['ZipCode']}")
        
    # Print all VENDOR_ITEM records
    print("VENDOR_ITEM table")
    m = match_query('VENDOR_ITEM')
    result = session.run(m)
    for record in result:
        print(f"iId: {record['n']['iId']}, vId: {record['n']['vId']}, Vprice: {record['n']['Vprice']}")
        
    # Print all ITEM records
    print("ITEM table")
    m = match_query('ITEM')
    result = session.run(m)
    for record in result:
        print(f"iId: {record['n']['iId']}, Iname: {record['n']['Iname']}, Sprice: {record['n']['Sprice']}")   
        
    # Print all ORDERS records
    print("ORDERS table")
    m = match_query('ORDERS')
    result = session.run(m)
    for record in result:
        print(f"oId: {record['n']['oId']}, sId: {record['n']['sId']}, cId: {record['n']['cId']}, Odate: {record['n']['Odate']}, Ddate: {record['n']['Ddate']}, Amount: {record['n']['Amount']}")
        
    # Print all CUSTOMER records
    print("CUSTOMER table")
    m = match_query('CUSTOMER')
    result = session.run(m)
    for record in result:
        print(f"cId: {record['n']['cId']}, Cname: {record['n']['Cname']}, Street: {record['n']['Street']}, City: {record['n']['City']}, StateAb: {record['n']['StateAb']}, Zipcode: {record['n']['Zipcode']}")
        
    # Print all STORE records
    print("STORE table")
    m = match_query('STORE')
    result = session.run(m)
    for record in result:
        print(f"sId: '{record['n']['sId']}', Sname: '{record['n']['Sname']}', Street: '{record['n']['Street']}', City: '{record['n']['City']}', StateAb: '{record['n']['StateAb']}', ZipCode: '{record['n']['ZipCode']}', Sdate: '{record['n']['Sdate']}', Telno: '{record['n']['Telno']}', URL: '{record['n']['URL']}'")
    
    # Print all ORDER_ITEM records
    print("ORDER_ITEM table")
    m = match_query('ORDER_ITEM')
    result = session.run(m)
    for record in result:
        print(f"oId: '{record['n']['oId']}', iId: '{record['n']['iId']}', Icount: {record['n']['Icount']}")  
        
    # Print all OLDPRICE records
    print("OLDPRICE table")
    m = match_query('OLDPRICE')
    result = session.run(m)
    for record in result:
        print(f"iId: '{record['n']['iId']}', Sprice: {record['n']['Sprice']}, Sdate: '{record['n']['Sdate']}', Edate: '{record['n']['Edate']}'")
        
    # Print all EMPLOYEE records
    print("EMPLOYEE table")
    m = match_query('EMPLOYEE')
    result = session.run(m)
    for record in result:
        print(f"sId: '{record['n']['sId']}', SSN: '{record['n']['SSN']}', Sname: '{record['n']['Sname']}', Street: '{record['n']['Street']}', City: '{record['n']['City']}', Bdate: '{record['n']['Bdate']}', StateAb: '{record['n']['StateAb']}', Zipcode: '{record['n']['Zipcode']}', Etype: '{record['n']['Etype']}', Sdate: '{record['n']['Sdate']}', Edate: '{record['n']['Edate']}', Level: '{record['n']['Level']}', Asalary: '{record['n']['Asalary']}', Agency: '{record['n']['Agency']}', Hsalary: {record['n']['Hsalary']}, Institute: '{record['n']['Institute']}', Itype: '{record['n']['Itype']}'")
    
    # Print all CONTRACT records
    print("CONTRACT table")
    m = match_query('CONTRACT')
    result = session.run(m)
    for record in result:
        print(f"vId: '{record['n']['vId']}', ctId: '{record['n']['ctId']}', Sdate: '{record['n']['Sdate']}', Ctime: '{record['n']['Ctime']}', Cname: '{record['n']['Cname']}'")

   
    # VENDOR SELLS ITEM 
    sells_query = '''
    MATCH (v:VENDOR), (vi:VENDOR_ITEM), (i:ITEM)
    WHERE v.vId = vi.vId AND vi.iId = i.iId
    MERGE (v)-[:SELLS]->(i)
    '''
    session.run(sells_query)
    
    # VENDOR_ITEM FOUND_IN VENDOR 
    foundin_query = '''
    MATCH (vi:VENDOR_ITEM), (v:VENDOR)
    WHERE vi.vId = v.vId
    MERGE (vi)-[:FOUND_IN]->(v)
    '''
    session.run(foundin_query)
    
    # ORDER BELONGS_TO CUSTOMER 
    belongsto_query = '''
    MATCH (o:ORDERS), (c:CUSTOMER)
    WHERE o.cId = c.cId
    MERGE (o)-[:BELONGS_TO]->(c)
    '''
    session.run(belongsto_query)
    
    # ORDER CONTAINS ITEM 
    contains_query = '''
    MATCH (o:ORDERS), (oi:ORDER_ITEM), (i:ITEM)
    WHERE o.oId = oi.oId AND oi.iId = i.iId
    MERGE (o)-[:CONTAINS]->(i)
    '''
    session.run(contains_query)
    
    #order_item MATCHES item
    matches_query = '''
    MATCH (oi:ORDER_ITEM), (i:ITEM)
    WHERE oi.iId = i.iId
    MERGE (oi)-[:MATCHES]->(i)
    '''
    session.run(matches_query)
    
    #Employee WORKS_AT Store
    works_query = '''
    MATCH (e:EMPLOYEE), (s:STORE)
    WHERE e.sId = s.sId
    MERGE (e)-[:WORKS_AT]->(s)
    '''
    session.run(works_query)
    
    #Vendor HAS Contract
    has_query = '''
    MATCH (v:VENDOR), (c:CONTRACT)
    WHERE v.vId = c.vId
    MERGE (v)-[:HAS]->(c)
    '''
    session.run(has_query)
    
    #Item HAD Oldprice
    had_query = '''
    MATCH (i:ITEM), (op:OLDPRICE)
    WHERE i.iId = op.iId
    MERGE (i)-[:HAD]->(op)
    '''
    session.run(had_query)  

driver.close()
# Reety Gyawali 1001803756 
# Rakshav Patel – 10011941754

# pip3 install neo4j-driver

from neo4j import GraphDatabase, basic_auth

# Create nodes for each file
def create_vendor_item(vId, iId, Vprice):
    return f"CREATE (:VENDOR_ITEM{{iId: '{iId}', vId: '{vId}', Vprice: '{Vprice}'}})"

def create_vendor(vId,Vname,Street,City,StateAb,ZipCode):
    return f"CREATE (:VENDOR{{vId: '{vId}', Vname: '{Vname}', Street: '{Street}', City: '{City}', StateAb: '{StateAb}', ZipCode: '{ZipCode}'}})"

def create_item(iId,Iname,Sprice):
    return f"CREATE (:ITEM{{iId: '{iId}', Iname: '{Iname}', Sprice: '{Sprice}'}})"

def create_order(oId,sId,cId,Odate,Ddate,Amount):
    return f"CREATE (:ORDERS{{oId: '{oId}', sId: '{sId}', cId: '{cId}', Odate: '{Odate}', Ddate: '{Ddate}', Amount: '{Amount}'}})"

def create_customer(cId,Cname,Street,City,StateAb,Zipcode):
    return f"CREATE (:CUSTOMER{{cId: '{cId}', Cname: '{Cname}', Street: '{Street}', City: '{City}', StateAb: '{StateAb}', Zipcode: '{Zipcode}'}})"

def create_store(sID,Sname,Street,City,StateAb,ZipCode,Sdate,Telno,URL):
    return f"CREATE(:STORE{{sID: '{sID}', Sname: '{Sname}', Street: '{Street}', City: '{City}', StateAb: '{StateAb}', ZipCode: '{ZipCode}', Sdate: '{Sdate}', Telno: '{Telno}', URL: '{URL}'}})"

def create_order_item(oId, iId, Icount):
    return f"CREATE (:ORDER_ITEM{{oId: '{oId}', iId: '{iId}', Icount: '{Icount}'}})"

def create_oldprice(iId, Sprice, Sdate, Edate):
    return f"CREATE(:OLDPRICE{{iId: '{iId}', Sprice: '{Sprice}', Sdate: '{Sdate}', Edate: '{Edate}'}})"

def create_employee(sId,SSN,Sname,Street,City,StateAb,Zipcode,Etype,Bdate,Sdate,Edate,Level,Asalary,Agency,Hsalary,Institute,Itype):
    return f"CREATE(:EMPLOYEE{{sId: '{sId}', SSN: '{SSN}', Sname: '{Sname}', Street: '{Street}', City: '{City}', Bdate: '{Bdate}', StateAb: '{StateAb}', Zipcode: '{Zipcode}', Etype: '{Etype}', Sdate: '{Sdate}', Edate: '{Edate}', Level: '{Level}', Asalary: '{Asalary}', Agency: '{Agency}', Hsalary: '{Hsalary}', Institute: '{Institute}', Itype: '{Itype}'}})"

def create_contract(vId,ctId,Sdate,Ctime,Cname):
    return f"CREATE(:CONTRACT{{vId: '{vId}', ctId: '{ctId}', Sdate: '{Sdate}', Ctime: '{Ctime}', Cname: '{Cname}'}})"

# Sandbox info (can replace with your own)
driver = GraphDatabase.driver(
  "bolt://3.84.30.196:7687",
  auth=basic_auth("neo4j", "purposes-administrators-washtub"))

with driver.session(database="neo4j") as session:
    # Open each file and create nodes
    with open("Sprouts Data/VENDOR.csv") as v_file:
        next(v_file) # Skip header line
        for line in v_file:
            vId,Vname,Street,City,StateAb,ZipCode = line.strip().split(',')
            query = create_vendor(vId,Vname,Street,City,StateAb,ZipCode)
            # Run query
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
            sID,Sname,Street,City,StateAb,ZipCode,Sdate,Telno,URL = line.strip().split(',')
            query = create_store(sID,Sname,Street,City,StateAb,ZipCode,Sdate,Telno,URL)
            session.run(query)   

    with open("Sprouts Data/ORDER_ITEM.csv") as order_item_file:
        next(order_item_file)
        for line in order_item_file:
            oId, iId, Icoun = line.strip().split(',')
            query = create_order_item(oId, iId, Icoun)
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
    WHERE oi.iID = i.iId
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

driver.close()
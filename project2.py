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


# Sandbox info (can replace with your own)
driver = GraphDatabase.driver(
  "bolt://174.129.159.245:7687",
  auth=basic_auth("neo4j", "capacities-laser-qualifiers"))

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
    
    # # ORDER CONTAINS ITEM 
    # contains_query = '''
    # MATCH (o:ORDERS), (oi:ORDER_ITEM), (i:ITEM)
    # WHERE o.oId = oi.oId AND oi.iId = i.iId
    # MERGE (o)-[:CONTAINS]->(i)
    # '''
    # session.run(contains_query)
    

driver.close()
# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import settings
import sys,os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql as db

def connection():
    ''' User this function to create your connections '''
    con = db.connect(
        settings.mysql_host, 
        settings.mysql_user, 
        settings.mysql_passwd, 
        settings.mysql_schema)
    
    return con

def findAirlinebyAge(x,y):
    
    # Create a new connection
    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    #query1 found the airline name and the num of passengers 
    query="""select  airlines.name as "airline_name",count(distinct passengers.id) as "num_of_passengers"
    from airlines,airlines_has_airplanes aha,airplanes,passengers,flights,flights_has_passengers fhp
    where aha.airlines_id=airlines.id AND aha.airplanes_id=airplanes.id AND fhp.flights_id=flights.id AND fhp.passengers_id=passengers.id
    AND flights.airplanes_id=airplanes.id AND 2022-passengers.year_of_birth>'%s' AND 2022-passengers.year_of_birth<'%s'
    group by airplanes.id 
    order by  num_of_passengers desc"""%(str(x),str(y))
    cur.execute(query)
    result1=cur.fetchone()
    query="""select count(distinct aha.airplanes_id) as "num_of_aircrafts"
    from airlines,airlines_has_airplanes aha,airplanes,passengers
    where aha.airlines_id=airlines.id AND aha.airplanes_id=airplanes.id AND airlines.name='%s'"""%(result1[0])
    cur.execute(query)
    result2=cur.fetchone()
    return [("airline_name","num_of_passengers", "num_of_aircrafts"),(result1[0],result1[1],result2[0])]


def findAirportVisitors(x,a,b):
    
   # Create a new connection
    con=connection()
    
    # Create a cursor on the connection
    cur=con.cursor()
    #MySQL
    query="""select airports.name,count(fhp.passengers_id)
    from airports,airlines,passengers,flights_has_passengers fhp,flights,routes
    where fhp.flights_id=flights.id AND fhp.passengers_id=passengers.id AND  routes.airlines_id=airlines.id 
    AND flights.routes_id=routes.id AND  routes.destination_id=airports.id 
    AND airlines.name='%s'  AND flights.date>'%s' AND flights.date<'%s' 
    group by airports.id """%(str(x),str(a),str(b))
    cur.execute(query)
    result=cur.fetchall()
    return [("aiport_name", "number_of_visitors"),*result]

def findFlights(x,a,b):

    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    #MySQL
    query="""select flights.id,airlines.alias,airports2.name,airplanes.model
    from flights,airlines,airports,airplanes,airlines_has_airplanes aha,routes,airports airports2
    where aha.airlines_id=airlines.id AND aha.airplanes_id=airplanes.id AND routes.airlines_id=airlines.id 
    AND flights.routes_id=routes.id AND flights.airplanes_id=airplanes.id AND routes.destination_id=airports2.id AND routes.source_id=airports.id 
    AND airlines.active="Y" AND flights.date='%s' AND airports.city='%s' AND airports2.city='%s'"""%(str(x),str(a),str(b))
    cur.execute(query)
    result=cur.fetchall()
    
    return [("flight_id", "alt_name", "dest_name", "aircraft_model"),*result]
    

def findLargestAirlines(N):
    # Create a new connection
    con=connection()

    # Create a cursor on the connection
    cur=con.cursor()
    query="""select airlines.name as "airline_name",airlines.code as "airlines_code",count(distinct flights.id) as "num_of_flights"
    from airlines,airlines_has_airplanes aha,airplanes,flights,routes
    where aha.airlines_id=airlines.id AND aha.airplanes_id=airplanes.id AND  flights.routes_id=routes.id 
    AND routes.airlines_id=airlines.id AND flights.airplanes_id=airplanes.id 
    group by airlines.id
    order by num_of_flights desc"""
    cur.execute(query)
    result=cur.fetchall()
    airlines={} #dictionary
    for (name,code,num_of_flights)in result:
        airlines[name]=(code,num_of_flights)
    query="""select airlines.name as "airline_name",airlines.code as "airlines_code",count(distinct aha.airplanes_id) as "num_of_aircrafts"
    from airlines,airlines_has_airplanes aha,airplanes
    where aha.airlines_id=airlines.id AND aha.airplanes_id=airplanes.id
    group by airlines.id"""
    results=[] #list
    cur.execute(query)
    
    result=cur.fetchall()
    for (name,code,num_of_aircrafts)in result:
        results.append((name,code,num_of_aircrafts,airlines[name][1]))
    results.sort(key =takeforth,reverse=True) #order by num_of_flights

    return [("name", "id", "num_of_aircrafts", "num_of_flights"),*results[:int(N)]]
 # take forth element for sort
def takeforth(elem):
    return elem[3]   
def insertNewRoute(x,y):
    # Create a new connection
    con=connection()

    # Create a cursor on the connection
    cur=con.cursor()
    
     query="""select a2.city as "Destination_city"
    from flights,routes,airports a1,airports a2,airlines
    where flights.routes_id=routes.id AND routes.airlines_id=airlines.id
    AND routes.source_id=a1.id AND routes.destination_id=a2.id
    AND airlines.alias='%s' AND a1.name='%s' """%(str(x),str(y)) #select the names from the cities the airline already has routes to
    cur.execute(query)
    result=cur.fetchall()
    
    query="""select airports.city
    from airports,routes
    where airports.id=routes.destination_id
    group by  airports.city """  #select the names from the cities that serve as destinations on the database
    cur.execute(query)
    result2=cur.fetchall()
    results = [x for x in result2 if x not in result] #keep only the  names from the cities(destination)  that the ailine doesn't already have a route to
    if not results:
        return ["airline capacity full",]
        
    query="""select airports.id
    from airports 
    where airports.city='%s' """%(results[0][0]) 
    cur.execute(query)
    result=cur.fetchall()
    
    query="""select airlines.id
    from airlines
    where airlines.name='%s' """%(str(x))
    cur.execute(query)
    result2=cur.fetchall()
    dest_id = result[0][0]
    airlines_id = result2[0][0]
    
    query="""select airports.id
    from airports
    where airports.name='%s' """%(str(y))
    cur.execute(query)
    result2=cur.fetchall()
    source_id = result2[0][0]
    
    query="""select routes.id
    from routes"""
    cur.execute(query)
    result=cur.fetchall()
    route_id = max([x[0] for x in result]) + 1 #to create a new unique routes.id, since ids are depicted as numbers, we get the max of them and add 1 to create a new max and thus unique new id
    
    query="""INSERT INTO routes (id,airlines_id,source_id,destination_id)
    VALUES (%d,%d,%d,%d)"""%(route_id,airlines_id,source_id,dest_id)
    cur.execute(query)
    result=cur.fetchall()
    return [("ok"),]

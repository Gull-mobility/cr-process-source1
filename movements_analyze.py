import datetime
import math

def calculate_movements(new_locations,old_locations):
    in_debug = False

    location_with_changes = {}
    movements = []
    counter_new = 0
    counter_change = 0

    #Para todas las nuevas posiciones comparamos con posicion anterior
    for plate in new_locations:
        print('[FUNC]check_movements') if in_debug else ''

        print('dict_old_locations:') if in_debug else ''
        print(old_locations) if in_debug else ''

        #Esto ya no haria falta, guardar al final
        #Check if plate exist in firebase database
        if plate not in old_locations.keys():
            print('[check_movements]New vehicle') if in_debug else ''
            print(new_locations) if in_debug else ''
            #If this vehicule do not exist not continue

            #Si es nueva la anadimos a firestore
            location_with_changes[plate] = new_locations[plate]
            #Add one to new counter
            counter_new = counter_new + 1
        else:

            new_latitude = new_locations[plate]['latitud']
            old_latitude = old_locations[plate]['latitud']

            new_longitude = new_locations[plate]['longitud']
            old_longitude = old_locations[plate]['longitud']


            if(new_latitude == old_latitude and new_longitude == old_longitude):
                print('[check_movements] Son iguales') if in_debug else ''
            else:
                print('[check_movements] Son distintas') if in_debug else ''
                print(new_latitude, old_latitude, new_longitude, old_longitude) if in_debug else ''

                #Calculate distance
                distance = calculate_distance(old_latitude, old_longitude, new_latitude, new_longitude)

                if distance > 0.2: #Move more than 200meters

                    #If charge_end > charge_start + 20 --> vehicle has recharged
                    recharge = False
                    if( new_locations[plate]['energia'] - old_locations[plate]['energia'] < 20):
                        recharge = True

                    #Prepare object of movements and add to list
                    movements.append(prepare_movement_object(new_locations[plate], old_locations[plate], round(distance,2), recharge))
                    
                    #Save the change in firestore
                    #location_with_changes[plate] = new_locations[plate]

                    #Add one to change counter
                    counter_change = counter_change + 1

        #Save the change in firestore
        #Now we want to save all the change becouse if not save the start of trip is not true
        location_with_changes[plate] = new_locations[plate]

    return location_with_changes, movements, counter_new, counter_change

#Build object to save in table 
def prepare_movement_object(new_location, old_location, distance, recharge):

    timestamp_start_format = old_location['timestamp']
    if type(timestamp_start_format) != str:
        timestamp_start_format =  old_location['timestamp'].strftime("%Y-%m-%d %H:%M:%S")

    movement_object = {
        "servicio": new_location['servicio'],
        "idVehiculo": new_location['idVehiculo'],
        "matricula": new_location['matricula'],
        "energia_start": old_location['energia'],
        "energia_end": new_location['energia'],
        "latitud_start": old_location['latitud'],
        "latitud_end": new_location['latitud'],
        "longitud_start": old_location['longitud'],
        "longitud_end": new_location['longitud'],
        "precio": new_location['precio'],
        "tipo": new_location['tipo'],
        "categoria": new_location['categoria'],
        "imagen": new_location['imagen'],
        "uoid_start": old_location['uoid'],
        "uoid_end": new_location['uoid'],
        "epochTime_start": old_location['epochTime'],
        "epochTime_end": new_location['epochTime'],
        "realTime_start": old_location['realTime'],
        "realTime_end": new_location['realTime'],
        "geo_start": old_location['geo'],
        "geo_end": new_location['geo'],
        "clusterId_start": old_location['clusterId'],
        "clusterId_end": new_location['clusterId'],
        "clusterLatitude_start": old_location['clusterLatitude'],
        "clusterLatitude_end": new_location['clusterLatitude'],
        "clusterLongitude_start": old_location['clusterLongitude'],
        "clusterLongitude_end": new_location['clusterLongitude'],
        "timestamp_start": timestamp_start_format,
        "timestamp_end": new_location['timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
        "tipoVehiculo": new_location['tipoVehiculo'],
        "distance" :distance,
        "recharge" : recharge
    } 

    return movement_object

def calculate_distance(lat1, lon1, lat2, lon2):

    R = 6371  # radius of the earth in km
    
    rad=math.pi/180
    dlat=lat2-lat1
    dlon=lon2-lon1
    RT=6372.795477598
    a=(math.sin(rad*dlat/2))**2 + math.cos(rad*lat1)*math.cos(rad*lat2)*(math.sin(rad*dlon/2))**2
    distance=2*RT*math.asin(math.sqrt(a))
    
    return distance
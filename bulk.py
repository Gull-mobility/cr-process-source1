from firestore import firestore_get_old_situation, firestore_write_changes
from bigquery import bigquery_positions_by_id, bigquery_save_movements, bigquery_bulk_uoid
from movements_analyze import calculate_movements

from datetime import datetime

#This class is only invoked to process old data, not in production with real data

#Get from firestore old location
old_location = firestore_get_old_situation()
print('Firestore has ' + str(len(old_location)) + ' vehicles')

#Get list of uoids
uoids_list = bigquery_bulk_uoid()

print('Analysing ' + str(len(uoids_list)) + ' uoids from bigquery')


for id,row_uoid in enumerate(uoids_list):

    #Format date
    uoid = row_uoid[0]
    uoid_date = row_uoid[1]
    uoid_date_formated = uoid_date.strftime("%Y-%m-%d")

    print(uoid_date_formated)

    #Get new positions
    new_locations = bigquery_positions_by_id(uoid, uoid_date_formated)
    #print('New locations from BigQuery ' + uoid + ' : ' + str(len(new_locations)))

    #Calculate movements
    locations_with_changes, movements, counter_new, counter_change = calculate_movements(new_locations, old_location)

    #Locantions can be bigger than movements because a new vehicle is not a movements
    #print('New vehicles: ' + str(counter_new) + '. Changes: ' + str(counter_change))
    #print(str(len(locations_with_changes)) + ' firestore changes, '  + str(len(movements)) + ' movements')

    #Save movements in bigquery
    if movements:
        bigquery_save_movements(movements)

    #Prepare old_location to next iteration
    for plate in locations_with_changes:
        old_location[plate] = locations_with_changes[plate]

    #Print status
    print(str(id+1) + ' of ' + str(len(uoids_list)) + '  -  ' + str(len(locations_with_changes)) + ' firestore changes, '  + str(len(movements)) + ' movements')

#Write in firestore the changes - Save all location

print('Save in firestore ' + str(len(old_location)) + ' vehicles')

firestore_write_changes(old_location)
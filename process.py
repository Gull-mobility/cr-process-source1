import datetime

def execution(uoid):
    #Import other clases
    from firestore import firestore_get_old_situation, firestore_write_changes
    from bigquery import bigquery_positions_by_id, bigquery_save_movements
    from movements_analyze import calculate_movements

    #Get last database locations
    old_location = firestore_get_old_situation()
    print('Firestore has ' + str(len(old_location)) + ' vehicles')

    #Prepare date - Today +/-1
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    tomorrow = (datetime.datetime.today() - datetime.timedelta(days=-1)).strftime('%Y-%m-%d')

    #Get new positions
    new_locations = bigquery_positions_by_id(uoid,yesterday,tomorrow)
    print('New locations from BigQuery ' + uoid + ' : ' + str(len(new_locations)))

    #Calculate movements
    locations_with_changes, movements, counter_new, counter_change = calculate_movements(new_locations, old_location)

    #Locantions can be bigger than movements because a new vehicle is not a movements
    print('New vehicles: ' + str(counter_new) + '. Changes: ' + str(counter_change))
    print(str(len(locations_with_changes)) + ' firestore changes, '  + str(len(movements)) + ' movements')

    #Save movements in bigquery
    if movements:
        bigquery_save_movements(movements)

    #Write in firestore the changes
    firestore_write_changes(locations_with_changes)
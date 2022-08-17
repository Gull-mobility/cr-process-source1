#Imports firestore
import firebase_admin
from firebase_admin import firestore

#CONSTANTS
firestore_actual_collection = "vehicles_current_a"

#Initialize firestore
firebase_admin.initialize_app()
db_firestore = firestore.client()

#Get old location from firestore
def firestore_get_old_situation():
  dict_old_locations = {}

  docs = db_firestore.collection(firestore_actual_collection).get()

  #TODO: FInd another idea to convert Firebase collection to python dict
  for doc in docs:
    #print(f'{doc.id} => {doc.to_dict()}')
    dict_old_locations[doc.id] = doc.to_dict()

  #print('Vehicles in Firestore: ' +  str(len(dict_old_locations)))
  return dict_old_locations

def firestore_write_changes(location_with_changes):



    #Prepare 400 items batches
    list_to_batch = []
    in_list = []
    for id,item in enumerate(location_with_changes):
        if(id%400==0):
            list_to_batch.append(in_list.copy())
            in_list = []
        in_list.append(item)
    #Last group also insert
    list_to_batch.append(in_list.copy())

    for items in list_to_batch:
        batch = db_firestore.batch()
        for vehicle_item in items:
            #print(list_to_batch[vehicle_item])
            fst_ref = db_firestore.collection(firestore_actual_collection).document(location_with_changes[vehicle_item]['matricula'])
            batch.set(fst_ref, location_with_changes[vehicle_item])
        # Finally commit the batch
        batch.commit()

    print('Saved in firestore')
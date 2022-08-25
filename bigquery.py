#Imports bigquery
from google.cloud import bigquery
import datetime

client = bigquery.Client()

#Movements table name
table_id_movements = "vacio-276411.mainDataset.trips_a"

#Get actual positions bigquery for uoid
def bigquery_positions_by_id(uoid, date_first, date_end):

    print('Query uoid:"' + uoid +'"')

    #Partition time is to access to information that BigQuery dont put yet in a partition
    query = ' '.join(("SELECT * FROM `vacio-276411.mainDataset.bulkData`"
                "WHERE DATE(timestamp) BETWEEN '" + date_first  + "' AND '" + date_end  + "'AND uoid = '" + uoid +"'",
                "OR (_PARTITIONTIME IS null AND uoid = '" + uoid +"')"))

    query_job = client.query(query)  # Make an API request.

    vehicles = {}
    for row in query_job:
        dictRow = dict(row)

        #Fix some campos that makes problems in fierstore format
        dictRow['energia'] = float(dictRow['energia'])
        dictRow['latitud'] = float(dictRow['latitud'])
        dictRow['longitud'] = float(dictRow['longitud'])
        dictRow['epochTime'] = float(dictRow['epochTime'])
        dictRow['clusterId'] = float(dictRow['clusterId'])
        dictRow['clusterLatitude'] = float(dictRow['clusterLatitude'])
        dictRow['clusterLongitude'] = float(dictRow['clusterLongitude'])

        #Plate reformat to fix a error saving / in firestore
        dictRow['matricula'] = dictRow['matricula'].replace('/','_')

        #Fix error with empty plates becouse we use it as key of firestore
        if dictRow['matricula'] != "":
            vehicles[dictRow['matricula']] = dictRow

    return vehicles


def bigquery_save_movements(movements):

    errors = client.insert_rows_json(table_id_movements, movements)

    #TODO: Change to error loging
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


#This function is only used by bulk class, not in production execution
def bigquery_bulk_uoid():


  query = """
      SELECT uoid, _PARTITIONTIME as pt, realTime FROM `vacio-276411.mainDataset.bulkData` 
      WHERE DATE(timestamp) = "2022-08-08"
      GROUP BY uoid, pt, realTime
      ORDER BY realTime ASC
      LIMIT 4
  """

  #Start day_ 03/30/2021 
  #ATENTION WITH YEAR

  #This query can be reduced using _PARTITIONTIME instead of timestamp
  #DONE "2010-01-01" AND  "2021-03-31

  query = """
      SELECT uoid, _PARTITIONTIME as pt, realTime FROM `vacio-276411.mainDataset.bulkData` 
       WHERE DATE(timestamp) BETWEEN "2010-04-01" AND  "2021-04-30"
      GROUP BY uoid, pt, realTime
      ORDER BY realTime ASC
  """

  query_job = client.query(query)  # Make an API request.



  list_uoids = []
  for row in query_job:
      #print("name={}".format(row[0]))
      list_uoids.append(row)

  return list_uoids
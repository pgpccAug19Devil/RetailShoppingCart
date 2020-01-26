import boto3
from datetime import datetime
import time

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import json
import decimal


 
s3_client=boto3.client("s3")

dynamodb = boto3.resource('dynamodb')
InvDet = dynamodb.Table('inventoryDetails')
InvAudit = dynamodb.Table('InventoryAudit')

def InsertInvAudit(fname,ts1,fs,status,before_rec,err) :
    InvAudit.put_item(
        Item={
            "FileName" : fname,
            "CreatedDate" : ts1,
            "FileSize"    : fs,
            "Status"      : status,
            "Recs_Inventory_Table" : before_rec,
            "Error"  : str(err)
        }
        )
def InsertInvdet(invkey,pname,desc,category,sname,area,city,cost,retailprice,qtyav,qtysold,createdate,updatedate)    :
    InvDet.put_item(
                  Item = {
                     "InventoryKey" :  invkey,
                      "ProductName"  : pname,
                      "Description"  : desc,
                      "Category"     : category,
                      "StoreName" : sname,
                      "Area"  : area,
                      "City" : city,
                      "Cost" : int(cost),
                      "RetailPrice" : int(retailprice),
                      "QuantityAvailable" : int(qtyav),
                      "QuantitySold": int(qtysold),
                      "CreatedDate"  : createdate,
                      "UpdatedDate"  : updatedate
                      },
                     )
def UpdateInvdet(invkey,pname,desc,category1,sname,area,city,cost,retailprice,qtyav,createdate,udate)    :
    
    q=int(qtyav)
    
    set_query = "Set ProductName = :pname1, Description = :desc1,Category = :category11,StoreName = :sname1, Area = :area1,Cost = :cost1, RetailPrice = :rp, QuantityAvailable = QuantityAvailable + :q,#cd = :c,#ud = :u"
                  
    InvDet.update_item(
                  Key = {
                     "InventoryKey" :  invkey
                        },
                  UpdateExpression=set_query,
                  ExpressionAttributeNames = {
                      "#cd" : "CreatedDate",
                      "#ud" : "UpdatedDate"
                      
                  },
                  ExpressionAttributeValues={
                      ':pname1' : pname,
                      ':desc1'  : desc,
                      ':category11' : category1,
                      ':sname1': sname,
                      ':area1': area,
                      ':c' : createdate,
                      ':u' : udate,
                      ':q' : q,
                      ':rp' : int(retailprice),
                      ':cost1' : int(cost)
                 },
                  ReturnValues="UPDATED_NEW"
                    )

                

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    s3_file_size = event['Records'][0]['s3']['object']['size']
    print("Size of S3 File is : " ,s3_file_size)
    print("Name of S3 File is : " ,s3_file_name)
    ts=datetime.now()
    dt_object= ts.strftime('%Y-%m-%d %H:%M:%S')
    print("date timestamp now at the begining of Lambda Invocation is ",dt_object)
    status1="Ready to insert in table"
    Invreccount=InvDet.item_count
    
    resp = s3_client.get_object(Bucket=bucket_name,Key=s3_file_name)
    data = resp['Body'].read().decode("utf-8")
    l=len(data)
    print("Fetched data from s3 file, the length is ",l)
    #If s3 file size is greater than 0 , then insert a rec in InvAudit table
    if int(s3_file_size) > 0 :
      InsertInvAudit(s3_file_name,dt_object,s3_file_size,status1,Invreccount,"none")
      print("inserted status in audit table with status and current count of records in Inventory detail table is  " , status1,Invreccount)
         
    #Split the data from s3 by lines
    inventory = data.split("\n")
    l=len(inventory)
    print("Splitted the csv file by lines , the length is now : " ,l)
    
    flag=0
    #Split each line of csv file by comma
    for inv in inventory :
          if flag == 1 :
             break
          len1=len(inv)
          inv=inv.strip()
          print("length of this line is ",len1)
          print(inv)
          inv_data = inv.split(",")
          
          if inv_data[0] !=""  :

              #Add it to DynamoDb
              count1=InvDet.item_count
              if count1 == 0 :
                print("Inventory details has 0 records , inserting new record", count1)
                print("Inv_data[0] is ", inv_data[0])
                try:
                    
                   InsertInvdet(int(inv_data[0]),inv_data[1],inv_data[2],inv_data[3],inv_data[4],inv_data[5],inv_data[6],inv_data[7],inv_data[8],inv_data[9],inv_data[10],inv_data[11],inv_data[12])
                   status1="Success"
                   error1="none"
                except Exception as e:
                   print("exception is ",str(e))
                   status1 = "failed"
                   error1 = str(e)
                   flag=1
              else :
               print("Inventory details has  records , updating record", count1)
               try:
                 
                  UpdateInvdet(int(inv_data[0]),inv_data[1],inv_data[2],inv_data[3],inv_data[4],inv_data[5],inv_data[6],inv_data[7],inv_data[8],inv_data[9],inv_data[10],inv_data[11])
                  
                  status1="Success"
                  error1="none"
                 
               except ClientError as e:
                #e1 = e.json()
                #if e.["Error"]["Code"] == "ConditionalCheckFailedException":
                   if "attribute that does not exist" in str(e):
                       try:
                         InsertInvdet(int(inv_data[0]),inv_data[1],inv_data[2],inv_data[3],inv_data[4],inv_data[5],inv_data[6],inv_data[7],inv_data[8],inv_data[9],inv_data[10],inv_data[11],inv_data[12])
                         status1="Success"
                         error1="none"
                       except Exception as e:
                          print("exception is ",str(e))
                          status1 = "failed"
                          error1 = str(e)
                          flag=1
                   else :
                       print("exception is ",str(e))
                       error1 = str(e)
                       status1="failed"
                       flag=1
                  
                   
               #else:
                #  raise
               else:
                print("UpdateItem succeeded:")
                
    print ('Total items in the table are ', InvDet.item_count)
    #ts = time.time() 
    ts = datetime.now()
    dt_object = ts.strftime('%Y-%m-%d %H:%M:%S')
    print("date timestamp now is ",dt_object)
    Invreccount2=InvDet.item_count
    InsertInvAudit(s3_file_name,dt_object,s3_file_size,status1,Invreccount2,error1)   
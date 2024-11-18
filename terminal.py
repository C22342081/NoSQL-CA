from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint

def connect_to_db():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['CitySustainability']
    return db

def main():
    db = connect_to_db()
    collection = db['WaterConsumption']
    
    while True:
        print("\nWelcome to the Water Consumption Database Application")
        print("1. Display all documents")
        print("2. Add new document")
        print("3. Update existing document")
        print("4. Delete existing document")
        print("5. Search documents")
        print("6. Exit")
        
        choice = input("Enter choice (1-6): ")
        
        if choice == '1':
            display_documents(collection)
        elif choice == '2':
            add_document(collection)
        elif choice == '3':
            update_document(collection)
        elif choice == '4':
            delete_document(collection)
        elif choice == '5':
            search_documents(collection)
        elif choice == '6':
            print("Exiting the application.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 6.")

def display_documents(collection):
    print("\nAll Documents in the WaterConsumption Collection:")
    for doc in collection.find():
        pprint.pprint(doc)

def add_document(collection):
    print("\nAdd a New Document")
    try:
        year = int(input("Enter year: "))
        region_name = input("Enter region name: ")
        
        # Initialize the water_consumption dictionary
        water_consumption = {}
        
        # Input mean consumption
        mean_value = float(input("Enter mean consumption value: "))
        mean_unit = input("Enter unit for mean consumption (e.g., litres): ")
        water_consumption['mean'] = {
            "label": "Mean consumption in litres per meter per day",
            "value": mean_value,
            "unit": mean_unit
        }
        
        # Input median consumption
        median_value = float(input("Enter median consumption value: "))
        median_unit = input("Enter unit for median consumption (e.g., litres): ")
        water_consumption['median'] = {
            "label": "Median consumption in litres per meter per day",
            "value": median_value,
            "unit": median_unit
        }
        
        # Construct the document
        document = {
            "year": year,
            "region": {
                "name": region_name
            },
            "water_consumption": water_consumption
        }
        
        # Insert the document into the collection
        result = collection.insert_one(document)
        print(f"Document inserted with _id: {result.inserted_id}")
    except Exception as e:
        print(f"An error occurred: {e}")

def update_document(collection):
    print("\nUpdate an Existing Document")
    try:
        year = int(input("Enter the year of the document to update: "))
        region_name = input("Enter the region name of the document to update: ")
        
        # Find the document
        doc = collection.find_one({"year": year, "region.name": region_name})
        if doc:
            print("Current Document:")
            pprint.pprint(doc)
            
            # Choose what to update
            print("\nWhat would you like to update?")
            print("1. Mean Consumption")
            print("2. Median Consumption")
            choice = input("Enter choice (1 or 2): ")
            
            if choice == '1':
                new_value = float(input("Enter new mean consumption value: "))
                new_unit = input("Enter new unit for mean consumption: ")
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "water_consumption.mean.value": new_value,
                        "water_consumption.mean.unit": new_unit
                    }}
                )
                print("Mean consumption updated successfully.")
            elif choice == '2':
                new_value = float(input("Enter new median consumption value: "))
                new_unit = input("Enter new unit for median consumption: ")
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {
                        "water_consumption.median.value": new_value,
                        "water_consumption.median.unit": new_unit
                    }}
                )
                print("Median consumption updated successfully.")
            else:
                print("Invalid choice.")
        else:
            print("Document not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_document(collection):
    print("\nDelete an Existing Document")
    try:
        year = int(input("Enter the year of the document to delete: "))
        region_name = input("Enter the region name of the document to delete: ")
        
        # Confirm deletion
        confirm = input("Are you sure you want to delete this document? (y/n): ")
        if confirm.lower() == 'y':
            result = collection.delete_one({"year": year, "region.name": region_name})
            if result.deleted_count > 0:
                print("Document deleted successfully.")
            else:
                print("Document not found.")
        else:
            print("Deletion cancelled.")
    except Exception as e:
        print(f"An error occurred: {e}")

def search_documents(collection):
    print("\nSearch Documents")
    try:
        print("Search by:")
        print("1. Year")
        print("2. Region Name")
        print("3. Mean Consumption Value Greater Than")
        print("4. Mean Consumption Value Less Than")
        print("5. Median Consumption Value Greater Than")
        print("6. Median Consumption Value Less Than")
        choice = input("Enter choice (1-6): ")
        
        if choice == '1':
            year = int(input("Enter the year to search for: "))
            query = {"year": year}
        elif choice == '2':
            region_name = input("Enter the region name to search for: ")
            query = {"region.name": {"$regex": region_name, "$options": 'i'}}
        elif choice == '3':
            value = float(input("Enter the minimum mean consumption value: "))
            query = {"water_consumption.mean.value": {"$gt": value}}
        elif choice == '4':
            value = float(input("Enter the maximum mean consumption value: "))
            query = {"water_consumption.mean.value": {"$lt": value}}
        elif choice == '5':
            value = float(input("Enter the minimum median consumption value: "))
            query = {"water_consumption.median.value": {"$gt": value}}
        elif choice == '6':
            value = float(input("Enter the maximum median consumption value: "))
            query = {"water_consumption.median.value": {"$lt": value}}
        else:
            print("Invalid choice.")
            return
        
        results = collection.find(query)
        count = 0
        for doc in results:
            pprint.pprint(doc)
            count += 1
        
        if count == 0:
            print("No documents found matching the criteria.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

from database import DATABASE 

def main():
    db = DATABASE()


    if db.connect():
        print("Connected to InfluxDB successfully.")
        
       
        db.meas = "UEReports"  

        print("Reading data...")
        db.read_data(train=True, limit=1000)  
        print("Data read from InfluxDB:")
        print(db.data)

    else:
        print("Failed to connect to InfluxDB.")

if __name__ == "__main__":
    main()

package main

import (
    "bufio"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "fmt"
    "log"
    "os"
    "strings"
    "strconv"
)

const (
    ESCAPE_STR = "exit"
)

func main() {
    if _, err := os.Stat(DATABASE_PATH); os.IsNotExist(err) {
        fmt.Println("Creating database file")
        _, err := os.Create(DATABASE_PATH)
        if err != nil {
            log.Fatal(err)
        }
    }
    log.Printf("Opening SQLite file %s\n", DATABASE_PATH)
    tempDB, err := sql.Open("sqlite3", DATABASE_PATH)
    if err != nil {
        log.Printf("main.go: failed to open database: %v\n", err)
        return
    }
    db := Database{tempDB}
    defer db.Close()
    log.Printf("Successfully opened database %s\n", DATABASE_PATH)
    input := bufio.NewScanner(os.Stdin)
    fmt.Print("Enter command: ")
    for input.Scan() {
        if input.Text() == ESCAPE_STR {
            return
        }
        args := strings.Fields(input.Text())
        processCommand(&db, args[0], args[1:])
        fmt.Print("Enter command: ")
    }
}

func processCommand(db *Database, command string, args []string) error {
    /*
     * Supported commands:
     * get (schedule/stops/weekly) keys...
     * display (trip/offering/bus/driver/stop/actualinfo/stopinfo)
     * add (trip/offering/bus/driver/stop/actualinfo/stopinfo) keys...
     * addofferings
     * delete (offer/bus) keys...
     * change (driver/bus) keys...
     */
    switch command {
        case "get": // Get a set of information given a set of keys
        switch args[0] {
            case "schedule":
            if len(args) != 4 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 4, len(args))
            }
            trips, offerings, err := db.GetSchedule(args[1], args[2], args[3])
            if err != nil {
                return err
            }
            for _, t := range(trips) {
                fmt.Println("Trip\n---")
                for _, o := range(offerings[t.TripNumber]) {
                    fmt.Println(o)
                }
            }
            case "stops":
            if len(args) != 2 {
              return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 2, len(args))
            }
            num, err := strconv.Atoi(args[1])
            if err != nil {
                return err
            }
            stops, err := db.GetStops(num)
            if err != nil {
              return err
            }
            for _, stop := range(stops) {
              fmt.Println(stop)
            }
            case "weekly":
            if len(args) != 3 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 3, len(args))
            }
            offerings, err := db.GetDriverWeeklySchedule(args[1], args[2])
            if err != nil {
                return err
            }
            for _, o := range(offerings) {
                fmt.Println(o)
            }
            default:
            return fmt.Errorf("Unknown command %q\n", args[0])
        }
        
        case "display": // Display a row in the table (for debugging)
        if len(args) != 1 {
            return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 1, len(args))
        }
        switch args[0] {
            case "trip":
            table, err := db.GetTripTable()
            if err != nil {
                return err
            }
            fmt.Println(table)
            case "offering":
            table, err := db.GetTripOfferingTable()
            if err != nil {
                return err
            }
            fmt.Println(table)
            case "bus":
            table, err := db.GetBusTable()
            if err != nil {
                return err
            }
            fmt.Println(table)
            case "driver":
            table, err := db.GetDriverTable()
            if err != nil {
                return err
            }
            fmt.Println(table)
            case "stop":
            table, err := db.GetStopTable()
            if err != nil {
                return err
            }
            fmt.Println(table)
            case "actualinfo":
            table, err := db.GetActualTripStopInfoTable()
            if err != nil {
                return err
            }
            fmt.Println(table)
            case "stopinfo":
            table, err := db.GetTripStopInfoTable()
            if err != nil {
                return err
            }
            fmt.Println(table)
            default:
            return fmt.Errorf("Unknown command %q\n", args[0])
        }

        case "add": // Add a row into the databases
          switch args[0] {
            case "trip":
              if len(args) != 4 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 4, len(args))
              }
              num, err := strconv.Atoi(args[1])
              if err != nil {
                  return err
              }
              err = db.AddTrip(num,args[2],args[3])
              if err != nil {
                return err
              }
            case "offering":
              if len(args) != 7 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 7, len(args))
              }
              err := db.AddOffering(toInt(args[1]), args[2], args[3], args[4], args[5], toInt(args[6]))
              if err != nil {
                return err
              }
            case "bus":
              if len(args) != 4 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 4, len(args))
              }
              err := db.AddBus(toInt(args[1]), args[2], toInt(args[3]))
              if err != nil {
                return err
              }
            case "driver":
              if len(args) != 3 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 3, len(args))
              }
              err := db.AddDriver(args[1], args[2])
              if err != nil {
                return err
              }
            case "stop":
              if len(args) != 3 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 3, len(args))
              }
              err := db.AddStop(toInt(args[1]), args[2])
              if err != nil {
                return err
              }
            case "actualinfo":
              if len(args) != 10 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 10, len(args))
              }
              err := db.AddActualTripStopInfo(toInt(args[1]),args[2],args[3],toInt(args[4]),args[5],args[6],args[7],toInt(args[8]),toInt(args[9]))
              if err != nil {
                return err
              }
            case "stopinfo":
              if len(args) != 5 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 5, len(args))
              }
              drivetime, _ := strconv.ParseFloat(args[4], 32)
              float32_drivetime := float32(drivetime)
              err := db.AddTripStopInfo(toInt(args[1]), toInt(args[2]), toInt(args[3]), float32_drivetime)
              if err != nil {
                return err
              }
          }
        
        case "addofferings": // Add a set of rows into the database
        if len(args) != 0 {
            return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 0, len(args))
        }
        input := bufio.NewScanner(os.Stdin)
        for input.Scan() {
            if input.Text() == ESCAPE_STR {
                return nil
            }
            args = strings.Fields(input.Text())
            if len(args) != 6 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 6, len(args))
            }
            tripNumber, err := strconv.Atoi(args[0])
            if err != nil {
                return err
            }
            date := args[1]
            scheduledStartTime := args[2]
            scheduledArrivalTime := args[3]
            driverName := args[4]
            busID, err := strconv.Atoi(args[5])
            if err != nil {
                return err
            }
            err = db.AddOffering(tripNumber, date, scheduledStartTime, scheduledArrivalTime, driverName, busID)
            if err != nil {
                return err
            }
        }
        
        case "delete": // Deletes a trip from the database
        switch args[0] {
            case "offer":
            if len(args) != 4 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 4, len(args))
            }
            tripNumber, err := strconv.Atoi(args[1])
            if err != nil {
                return err
            }
            db.DeleteOffering(tripNumber, args[2], args[3])
            case "bus":
            if len(args) != 2 {
              return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n",2,len(args))
            }
            db.DeleteBus(toInt(args[1]))
        }
        case "change": // Change the driver or bus for a trip
        switch args[0] {
            case "driver":
            if len(args) != 5 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 5, len(args))
            }
            tripNumber, err := strconv.Atoi(args[2])
            if err != nil {
                return err
            }
            return db.ChangeDriver(args[1], tripNumber, args[3], args[4])
            case "bus":
            if len(args) != 5 {
                return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 5, len(args))
            }
            busID, err := strconv.Atoi(args[2])
            if err != nil {
                return err
            }
            tripNumber, err := strconv.Atoi(args[2])
            if err != nil {
                return err
            }
            return db.ChangeBus(busID, tripNumber, args[3], args[4])
        }
        default:
        fmt.Errorf("Unknown command %q\n", command)
    }
  return nil
}

func toInt(s string) int {
  i, _ := strconv.Atoi(s)
  return i
}

func (db *Database) GetTripTable() ([]Trip, error) {
    result := []Trip{}
    row, err := db.Query("SELECT * FROM Trip")
    if err != nil {
        return result, err
    }
    defer row.Close()
    result = RowToTrips(row)
    return result, nil
}

func (db *Database) GetTripOfferingTable() ([]TripOffering, error) {
    result := []TripOffering{}
    row, err := db.Query("SELECT * FROM TripOffering")
    if err != nil {
        return result, err
    }
    defer row.Close()
    result = RowToTripOfferings(row)
    return result, nil
}

func (db *Database) GetDriverTable() ([]Driver, error) {
    result := []Driver{}
    row, err := db.Query("SELECT * FROM Driver")
    if err != nil {
        return result, err
    }
    defer row.Close()
    result = RowToDrivers(row)
    return result, nil
}

func (db *Database) GetStopTable() ([]Stop, error) {
    result := []Stop{}
    row, err := db.Query("SELECT * FROM Stop")
    if err != nil {
        return result, err
    }
    defer row.Close()
    result = RowToStops(row)
    return result, nil
}

func (db *Database) GetActualTripStopInfoTable() ([]ActualTripStopInfo, error) {
    result := []ActualTripStopInfo{}
    row, err := db.Query("SELECT * FROM Stop")
    if err != nil {
        return result, err
    }
    defer row.Close()
    result = RowToActualStopInfos(row)
    return result, nil
}

func (db *Database) GetBusTable() ([]Bus, error) {
    result := []Bus{}
    row, err := db.Query("SELECT * FROM Bus")
    if err != nil {
        return result, err
    }
    defer row.Close()
    result = RowToBuses(row)
    return result, nil
}

func (db *Database) GetTripStopInfoTable() ([]TripStopInfo, error) {
    result := []TripStopInfo{}
    row, err := db.Query("SELECT * FROM TripStopInfo")
    if err != nil {
        return result, err
    }
    defer row.Close()
    result = RowToTripStopInfos(row)
    return result, nil
}
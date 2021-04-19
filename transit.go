// Database interactivity required for the lab
package main

import (
    "database/sql"
    "fmt"
    _ "github.com/mattn/go-sqlite3"
    "log"
    "time"
)

const (
    DATABASE_PATH = "./Lab4.db"
    DATE_FORMAT   = time.RFC3339
)

type Trip struct {
    TripNumber        int
    StartLocationName string
    DestinationName   string
}

type TripOffering struct {
    TripNumber           int
    Date                 string
    ScheduledStartTime   string
    ScheduledArrivalTime string
    DriverName           string
    BusID                int
}

type Bus struct {
    BusID int
    Model string
    Year  uint
}

type Driver struct {
    DriverName            string
    DriverTelephoneNumber string
}

type Stop struct {
    StopNumber  int
    StopAddress int
}

type ActualTripStopInfo struct {
    TripNumber           int
    Date                 string
    ScheduledStartTime   string
    StopNumber           int
    ScheduledArrivalTime string
    ActualStartTime      string
    ActualArrivalTime    string
    NumberOfPassengerIn  int
    NumberOfPassengerOut int
}

type TripStopInfo struct {
    TripNumber     int
    StopNumber     int
    SequenceNumber int
    DrivingTime    int
}

type Database struct {
    *sql.DB
}

// GetSchedule returns all trip offerings for the given information
func (db *Database) GetSchedule(startLocationName, destinationName, date string) ([]Trip, map[int][]TripOffering) {
    row, err := db.Query(fmt.Sprintf("SELECT * FROM Trip WHERE StartLocationName=%s", startLocationName))
    if err != nil {
        log.Fatal(err)
    }
    // Get the trips with the given start location name
    trips := []Trip{}
    for row.Next() {
        var tripNumber int
        var startLocationName string
        var destinationName string
        row.Scan(&tripNumber, &startLocationName, &destinationName)
        trips = append(trips, Trip{
            TripNumber:        tripNumber,
            StartLocationName: startLocationName,
            DestinationName:   destinationName,
        })
    }
    // Get the trip offerings for each trip
    offerings := make(map[int][]TripOffering)
    for _, t := range trips {
        row, err := db.Query(fmt.Sprintf("SELECT * FROM TripOffering WHERE TripNumber=%d", t.TripNumber))
        if err != nil {
            log.Fatal(err)
        }
        for row.Next() {
            var tripNumber int
            var date string
            var scheduledStartTime string
            var scheduledArrivalTime string
            var driverName string
            var busID int
            row.Scan(&tripNumber, &date, &scheduledStartTime, &scheduledArrivalTime, &driverName, &busID)
            if _, ok := offerings[tripNumber]; !ok {
                offerings[tripNumber] = []TripOffering{}
            }
            offerings[tripNumber] = append(offerings[tripNumber], TripOffering{
                TripNumber:           tripNumber,
                Date:                 date,
                ScheduledStartTime:   scheduledStartTime,
                ScheduledArrivalTime: scheduledArrivalTime,
                DriverName:           driverName,
                BusID:                busID,
            })
        }
    }
    return trips, offerings
}

// DeleteOffering deletes the trip offering with the given primary keys
func (db *Database) DeleteOffering(tripNumber int, date string, scheduledStartTime string) error {
    _, err := db.Query(fmt.Sprintf("DELETE FROM TripOffering WHERE TripNumber=%d AND Date=%s AND ScheduledStartTime=%s", tripNumber, date, scheduledStartTime))
    return err
}

// AddOfferings adds the set of offerings to the TripOffering table
func (db *Database) AddOfferings(offerings []TripOffering) error {
    for _, offer := range offerings {
        _, err := db.Query("INSERT INTO TripOffering (TripNumber, Date, ScheduledStartTime, ScheduledArrivalTime, DriverName, BusID) VALUES (%d, %q, %q, %q, %q, %d)", offer.TripNumber, offer.Date, offer.ScheduledStartTime, offer.ScheduledArrivalTime, offer.DriverName, offer.BusID)
        if err != nil {
            return err
        }
    }
    return nil
}

// ChangeDriver will change the driverName of the driver of the trip given by the composite key info
func (db *Database) ChangeDriver(driverName string, tripNumber int, date string, scheduledStartTime string) error {
    _, err := db.Query(fmt.Sprintf("UPDATE Trip SET DriverName=%q WHERE TripNumber=%d AND Date=%q AND ScheduledStartTime=%q", driverName, tripNumber, date, scheduledStartTime))
    return err
}

// GetStops returns all stops for a given trip number
func (db *Database) GetStops(tripNumber int) []TripStopInfo {
    //SELECT TSI
    //FROM TripStopInfo TSI
    //WHERE TSI.TripNumber = tripNumber
    row, err := db.Query(fmt.Sprintf("SELECT * FROM TripStopInfo TSI WHERE TSI.TripNumber = %d", tripNumber))
    if err != nil {
        log.Fatal(err)
    }
    stops := []TripStopInfo{}
    for row.Next() {
        var TripNumber int
        var StopNumber int
        var SequenceNumber int
        var DrivingTime int
        row.Scan(&TripNumber, &StopNumber, &SequenceNumber, &DrivingTime)
        stops = append(stops, TripStopInfo{
            TripNumber:     TripNumber,
            StopNumber:     StopNumber,
            SequenceNumber: SequenceNumber,
            DrivingTime:    DrivingTime,
        })
    }
    return stops
}

// Get the weekly schedule for a given driver and date
func (db *Database) GetDriverWeeklySchedule(driverName string, date string) []TripOffering {
    sameWeek := func(t1, t2 *time.Time) bool {
        year1, week1 := t1.ISOWeek()
        year2, week2 := t2.ISOWeek()
        return year1 == year2 && week1 == week2
    }
    row, err := db.Query("SELECT * FROM TripOffering WHERE DriverName=%q", driverName)
    date1, err := time.Parse(DATE_FORMAT, date)
    if err != nil {
        log.Fatal(err)
    }
    result := []TripOffering{}
    if err != nil {
        log.Fatal(err)
    }
    for row.Next() {
        var tripNumber int
        var date string
        var scheduledStartTime string
        var scheduledArrivalTime string
        var driverName string
        var busID int
        row.Scan(&tripNumber, &date, &scheduledStartTime, &scheduledArrivalTime, &driverName, &busID)
        date2, err := time.Parse(DATE_FORMAT, date)
        if err != nil {
            log.Fatal(err)
        }
        if sameWeek(&date1, &date2) {
            result = append(result, TripOffering{
                TripNumber:           tripNumber,
                Date:                 date,
                ScheduledStartTime:   scheduledStartTime,
                ScheduledArrivalTime: scheduledArrivalTime,
                DriverName:           driverName,
                BusID:                busID,
            })
        }
    }
    return result
}

// AddDriver adds a driver to the SQLite database
func (db *Database) AddDriver(driverName string, driverTelephoneNumber string) error {
    _, err := db.Query(fmt.Sprintf("INSERT INTO Driver (DriverName, DriverTelephoneNumber) VALUES(%q, %q)", driverName, driverTelephoneNumber))
    return err
}

// AddBus adds a bus to the SQLite database, returning err if falied
func (db *Database) AddBus(busID int, model string, year uint) error {
    _, err := db.Query(fmt.Sprintf("INSERT INTO Bus (BusID, Model, year) VALUES(%d, %q, %d)", busID, model, year))
    return err
}

// DeleteBus deletes a bus from the SQLite database, returning err if failed
func (db *Database) DeleteBus(busID int) error {
    _, err := db.Query(fmt.Sprintf("DELETE FROM Bus WHERE BusID = %d", busID))
    return err
}

func (db *Database) AddTripStopInfo(tripNumber int, stopNumber int, sequenceNumber int, drivingTime int) error {
    _, err := db.Query(fmt.Sprintf("INSERT INTO TripStopInfo (TripNumber, StopNumber, SequenceNumber, DrivingTime) Values (%d, %d, %d, %d)"), tripNumber, stopNumber, sequenceNumber, drivingTime)
    return err
}

func (db *Database) AddActualTripStopInfo(tripNumber int, date string, scheduledStartTime string, stopNumber int, scheduledArrivalTime string, actualStartTime string, actualArrivalTime string, numberOfPassengerIn int, numberOfPassengerOut int) error {
    _, err := db.Query(fmt.Sprintf("INSERT INTO ActualTripStopInfo (TripNumber, Date, ScheduledStartTime, StopNumber, ScheduledArrivalTime, ActualStartTime, ActualArrivalTime, NumberOfPassengerIn, NumberOfPassengerOut) VALUES (%d, %q, %q, %d, %q, %q, %q, %d, %d)"), tripNumber, date, scheduledStartTime, stopNumber, scheduledArrivalTime, actualStartTime, actualArrivalTime, numberOfPassengerIn, numberOfPassengerOut)
    return err
}

func (db *Database) AddTrip(tripNumber int, startLocationName string, destinationName string) error {
    _, err := db.Query(fmt.Sprintf("INSERT INTO Trip (TripNumber, StartLocationName,DestinationName) VALUES(%d,%q,%q)"), tripNumber, startLocationName, destinationName)
    return err
}
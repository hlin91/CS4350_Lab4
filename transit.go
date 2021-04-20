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

func (t Trip) String() string {
    return fmt.Sprintf("TripNumber: %d\nStartLocationName: %s\nDestinationName: %s", t.TripNumber, t.StartLocationName, t.DestinationName)
}

type TripOffering struct {
    TripNumber           int
    Date                 string
    ScheduledStartTime   string
    ScheduledArrivalTime string
    DriverName           string
    BusID                int
}

func (t TripOffering) String() string {
    return fmt.Sprintf("TripNumber: %d\nDate: %s\nScheduledStartTime: %s\nScheduledArrivalTime: %s\nDriverName: %s\nBusID: %d", t.TripNumber, t.Date, t.ScheduledStartTime, t.ScheduledArrivalTime, t.DriverName, t.BusID)
}

type Bus struct {
    BusID int
    Model string
    Year  uint
}

func (b Bus) String() string {
    return fmt.Sprintf("BusID: %d\nModel: %s\nYear: %d", b.BusID, b.Model, b.Year)
}

type Driver struct {
    DriverName            string
    DriverTelephoneNumber string
}

func (d Driver) String() string {
    return fmt.Sprintf("DriverName: %s\nDriverTelephoneNumber: %s", d.DriverName, d.DriverTelephoneNumber)
}

type Stop struct {
    StopNumber  int
    StopAddress string
}

func (s Stop) String() string {
    return fmt.Sprintf("StopNumber: %d\nStopAddress: %s", s.StopNumber, s.StopAddress)
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

func (a ActualTripStopInfo) String() string {
    return fmt.Sprintf("TripNumber: %d\nDate: %s\nScheduledStartTime: %s\nStopNumber: %d\nScheduledArrivalTime: %s\nActualStartTime: %s\nActualArrivalTime: %s\nNumberOfPassengerIn: %d\nNumberOfPassengerOut: %d", a.TripNumber, a.Date, a.ScheduledStartTime, a.StopNumber, a.ScheduledArrivalTime, a.ActualStartTime, a.ActualArrivalTime, a.NumberOfPassengerIn, a.NumberOfPassengerOut)
}

type TripStopInfo struct {
    TripNumber     int
    StopNumber     int
    SequenceNumber int
    DrivingTime    float32
}

func (t TripStopInfo) String() string {
    return fmt.Sprintf("TripNumber: %d\nStopNumber: %d\nSequenceNumber: %d\nDrivingTime: %d")
}

type Database struct {
    *sql.DB
}

// GetSchedule returns all trip offerings for the given information
func (db *Database) GetSchedule(startLocationName, destinationName, date string) ([]Trip, map[int][]TripOffering, error) {
    trips := []Trip{}
    offerings := make(map[int][]TripOffering)
    row, err := db.Query(fmt.Sprintf("SELECT * FROM Trip WHERE StartLocationName=%s", startLocationName))
    if err != nil {
        return trips, offerings, err
    }
    // Get the trips with the given start location name
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
    row.Close()
    // Get the trip offerings for each trip
    for _, t := range trips {
        row, err := db.Query(fmt.Sprintf("SELECT * FROM TripOffering WHERE TripNumber=%d", t.TripNumber))
        if err != nil {
            return trips, offerings, err
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
        row.Close()
    }
    return trips, offerings, nil
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
func (db *Database) GetStops(tripNumber int) ([]TripStopInfo, error) {
    //SELECT TSI
    //FROM TripStopInfo TSI
    //WHERE TSI.TripNumber = tripNumber
    stops := []TripStopInfo{}
    row, err := db.Query(fmt.Sprintf("SELECT * FROM TripStopInfo TSI WHERE TSI.TripNumber = %d", tripNumber))
    if err != nil {
        return stops, err
    }
    defer row.Close()
    for row.Next() {
        var TripNumber int
        var StopNumber int
        var SequenceNumber int
        var DrivingTime float32
        row.Scan(&TripNumber, &StopNumber, &SequenceNumber, &DrivingTime)
        stops = append(stops, TripStopInfo{
            TripNumber:     TripNumber,
            StopNumber:     StopNumber,
            SequenceNumber: SequenceNumber,
            DrivingTime:    DrivingTime,
        })
    }
    return stops, nil
}

// Get the weekly schedule for a given driver and date
func (db *Database) GetDriverWeeklySchedule(driverName string, date string) ([]TripOffering, error) {
    result := []TripOffering{}
    sameWeek := func(t1, t2 *time.Time) bool {
        year1, week1 := t1.ISOWeek()
        year2, week2 := t2.ISOWeek()
        return year1 == year2 && week1 == week2
    }
    row, err := db.Query("SELECT * FROM TripOffering WHERE DriverName=%q", driverName)
    if err != nil {
        return result, err
    }
    defer row.Close()
    date1, err := time.Parse(DATE_FORMAT, date)
    if err != nil {
        return result, err
    }
    if err != nil {
        return result, err
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
    return result, nil
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

func (db *Database) AddTripStopInfo(tripNumber int, stopNumber int, sequenceNumber int, drivingTime float32) error {
    _, err := db.Query(fmt.Sprintf("INSERT INTO TripStopInfo (TripNumber, StopNumber, SequenceNumber, DrivingTime) Values (%d, %d, %d, %f)"), tripNumber, stopNumber, sequenceNumber, drivingTime)
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

func (db *Database) AddStop(stopNumber int, stopAddress string) error {
    _, err := db.Query(fmt.Sprintf("INSERT INTO Stop (StopNumber, StopAddress) VALUES (%d, %q)", stopNumber, stopAddress))
    return err
}
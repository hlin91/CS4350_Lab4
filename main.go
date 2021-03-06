package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/hlin91/CS4350_Lab4/transit"
)

const (
	ESCAPE_STR = "exit"
)

func main() {
	db, err := transit.GetDatabase()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	log.Printf("Successfully opened database %s\n", transit.DATABASE_PATH)
	input := bufio.NewScanner(os.Stdin)
	fmt.Print("Enter command: ")
	for input.Scan() {
		if input.Text() == ESCAPE_STR {
			return
		}
		args := strings.Fields(input.Text())
		err := processCommand(db, args[0], args[1:])
		if err != nil {
			fmt.Println(err)
		}
		fmt.Print("Enter command: ")
	}
}

func processCommand(db *transit.Database, command string, args []string) error {
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
			for _, t := range trips {
				fmt.Println("Trip\n---")
				for _, o := range offerings[t.TripNumber] {
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
			for _, stop := range stops {
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
			for _, o := range offerings {
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
            forPrint := []fmt.Stringer{}
            for _, t := range table {
                forPrint = append(forPrint, fmt.Stringer(t))
            }
			PrettyPrintTable(forPrint)
		case "offering":
			table, err := db.GetTripOfferingTable()
			if err != nil {
				return err
			}
            forPrint := []fmt.Stringer{}
            for _, t := range table {
                forPrint = append(forPrint, fmt.Stringer(t))
            }
			PrettyPrintTable(forPrint)
		case "bus":
			table, err := db.GetBusTable()
			if err != nil {
				return err
			}
			forPrint := []fmt.Stringer{}
            for _, t := range table {
                forPrint = append(forPrint, fmt.Stringer(t))
            }
			PrettyPrintTable(forPrint)
		case "driver":
            table, err := db.GetDriverTable()
            if err != nil {
                return err
            }
            forPrint := []fmt.Stringer{}
            for _, t := range table {
                forPrint = append(forPrint, fmt.Stringer(t))
            }
			PrettyPrintTable(forPrint)
		case "stop":
			table, err := db.GetStopTable()
			if err != nil {
				return err
			}
			forPrint := []fmt.Stringer{}
            for _, t := range table {
                forPrint = append(forPrint, fmt.Stringer(t))
            }
			PrettyPrintTable(forPrint)
		case "actualinfo":
			table, err := db.GetActualTripStopInfoTable()
			if err != nil {
				return err
			}
			forPrint := []fmt.Stringer{}
            for _, t := range table {
                forPrint = append(forPrint, fmt.Stringer(t))
            }
			PrettyPrintTable(forPrint)
		case "stopinfo":
			table, err := db.GetTripStopInfoTable()
			if err != nil {
				return err
			}
			forPrint := []fmt.Stringer{}
            for _, t := range table {
                forPrint = append(forPrint, fmt.Stringer(t))
            }
			PrettyPrintTable(forPrint)
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
			err = db.AddTrip(num, args[2], args[3])
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
			err := db.AddActualTripStopInfo(toInt(args[1]), args[2], args[3], toInt(args[4]), args[5], args[6], args[7], toInt(args[8]), toInt(args[9]))
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
				return fmt.Errorf("Wrong number of arguments passed. Expected %d, got %d\n", 2, len(args))
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

// PrettyPrintTable pretty prints a table
func PrettyPrintTable(table []fmt.Stringer) {
    fmt.Println("=====================================================")
    for _, el := range table {
        fmt.Println(el)
    }
    fmt.Println("=====================================================")
}
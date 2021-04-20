create table Bus (
	BusID INT,
	Model VARCHAR(50),
	Year VARCHAR(50)
);

create table Driver (
	DriverName VARCHAR(50),
	DriverTelephoneNumber VARCHAR(50)
);

create table Stop (
	StopNumber INT,
	StopAddress VARCHAR(50)
);

create table Trip (
	TripNumber INT,
	StartLocationName VARCHAR(50),
	DestinationName VARCHAR(50)
);

create table TripOffering (
	TripNumber INT,
	Date DATE,
	ScheduledStartTime VARCHAR(50),
	ScheduledArrivalTime VARCHAR(50),
	DriverName VARCHAR(50),
	BusID INT
);

create table TripStopInfo (
	TripNumber INT,
	StopNumber INT,
	SequenceNumber INT,
	DrivingTime DECIMAL(4,1)
);

create table ActualTripStopInfo (
	TripNumber INT,
	Date DATE,
	ScheduledStartTime VARCHAR(50),
	StopNumber INT,
	ScheduledArrivalTime VARCHAR(50),
	ActualStartTime VARCHAR(50),
	ActualArrivalTime VARCHAR(50),
	NumberOfPassengersIn INT,
	NumberOfPassengersOut INT
);

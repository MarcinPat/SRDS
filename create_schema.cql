CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE FreeSeats (
	flightId int,
	seat int,
	PRIMARY KEY (flightId, seat)
);

CREATE TABLE OccupiedSeats (
	flightId int,
	seat int,
	customer varchar,
	czas timestamp,
	PRIMARY KEY (flightId, seat, czas, customer)
);

CREATE TABLE CustomersReservations (
	customer varchar,
	flightId int,
	seat int,
	PRIMARY KEY (customer, flightId, seat)
);

CREATE TABLE Airplanes (
	name varchar,
	seatCount int,
	PRIMARY KEY (name)
);

CREATE TABLE Flights (
	id int,
	name varchar,
	origin varchar,
	destination varchar,
	PRIMARY KEY (id)

);

insert into airplanes (name, seatCount) values ('A300', 300);
insert into airplanes (name, seatCount) values ('B300', 220);
insert into airplanes (name, seatCount) values ('C200', 200);



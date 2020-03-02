package cassdemo.backend;

import java.util.*;

public class FlightConsole {
    public static BackendSession backendSession;
    public static final int stressedFlightID = 777;

    private static Role role;

    private enum Role {
        Officer,
        Customer
    }






    private static void addFlight(Scanner reader) throws BackendException
    {
        List<Airplane> planes =  backendSession.getAvailableAirplanes();
        Map<String, Integer> planesMap = new HashMap<>();
        for (Airplane plane : planes){
            System.out.println(String.format("%s %d", plane.name, plane.seatCount));
            planesMap.put(plane.name, plane.seatCount);
        }

        // print available airplanes
        System.out.println("Issue: id name origin destination");
        String[] command = reader.nextLine().split(" ");
        backendSession.addFlight(
                Integer.parseInt(command[0]), command[1],
                planesMap.get(command[1]), command[2], command[3]);
    }

    private static void getBoardingList(Scanner reader) throws BackendException
    {
        // print available airplanes
        List<Integer> flights = backendSession.getAllFlights();
        for (Integer id : flights){
            System.out.println(id);
        }
        System.out.println("Issue: flightId");
        Flight flight = backendSession.getFlight(Integer.parseInt(reader.nextLine()));
        System.out.println(flight.Print());
    }

    private static void book(Scanner reader) throws BackendException
    {
        List<Integer> flights = backendSession.getAllFlights();
        System.out.printf("Available flights: ");
        for (Integer id : flights){
            System.out.println(id);
        }
        System.out.println("Issue: flightId, customerName, count");
        String[] command = reader.nextLine().split(" ");
        backendSession.bookSeats(Integer.parseInt(command[0]),
                command[1], Integer.parseInt(command[2]));
    }

    

    private static void getAllReservations(Scanner reader) throws BackendException
    {
        System.out.println("Issue: customerName");
        List<SeatID> reservations = backendSession.getAllCustomerReservations(reader.nextLine());
        for(SeatID seat : reservations) {
            System.out.println(
                    String.format(
                            "Flight: %d, Seat: %d", seat.FlightId, seat.SeatNo));
        }
        // add flight
    }

    public static void runCustomer() {
        String name = UUID.randomUUID().toString();
        for(int i = 0; i < 1; i++) {
            try {
                if (backendSession.bookSeats(
                        stressedFlightID, name, new Random().nextInt(299) + 1)) {
                   // System.out.println(String.format("Customer [%s] booked seat(s)", name));
                } else {
                   // System.out.println(String.format("Customer [%s] could not book seats :((", name));
                }
            } catch (BackendException e) {
                e.printStackTrace();
            }
        }

        
    }   // koniec public static void runCustomer() 

    private static void createStressedFlight() throws BackendException {
        backendSession.addFlight(stressedFlightID, "stressTest", 300,
                "Washington", "Chicago");
    }


    public static void stressTest() {
        //UUID.randomUUID().toString();
        try {
            createStressedFlight();
            for(int i = 0; i < 500; i++) {
                Thread t = new Thread() {
                    public void run() {
                        runCustomer();
                    }
                };
                t.start();
            }
        } catch (BackendException e) {
            e.printStackTrace();
        }

    }
}

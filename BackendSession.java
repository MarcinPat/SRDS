package cassdemo.backend;

//import com.sun.rowset.internal.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

//import java.sql.PreparedStatement;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Date;

import static java.time.LocalDate.now;
import java.util.concurrent.TimeUnit;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	public static BackendSession instance = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement SELECT_FROM_FREE_SEATS_BY_FLIGHT;
	private static PreparedStatement INSERT_INTO_FREE_SEATS;
	private static PreparedStatement DELETE_FROM_FREE_SEATS;

	private static PreparedStatement INSERT_INTO_OCCUPIED_SEATS;
	private static PreparedStatement SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHT;
	private static PreparedStatement DELETE_FROM_OCCUPIED_SEATS;
	
	private static PreparedStatement SELECT_FROM_OCCUPIED_SEATS_BY_COUNT;
	private static PreparedStatement SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHTID_AND_SEAT;
	
	

	private static PreparedStatement INSERT_INTO_CUSTOMERS_RESERVATIONS;
	private static PreparedStatement SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER;
	private static PreparedStatement SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER_AND_FLIGHT;
	private static PreparedStatement DELETE_FROM_CUSTOMERS_RESERVATIONS;

	private static PreparedStatement INSERT_INTO_FLIGHTS;
	private static PreparedStatement SELECT_FROM_FLIGHTS;

	private static PreparedStatement SELECT_FROM_AIRPLANES;


	private void prepareStatements() throws BackendException {
		try {
			SELECT_FROM_FREE_SEATS_BY_FLIGHT = session.prepare(
					"SELECT * FROM FreeSeats WHERE flightId = ?;");
			INSERT_INTO_FREE_SEATS = session.prepare(
					"INSERT INTO FreeSeats (flightId, seat) VALUES (?, ?);");
			DELETE_FROM_FREE_SEATS = session.prepare(
					"DELETE FROM FreeSeats WHERE flightId = ? AND seat = ?;");

			SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHT = session.prepare(
					"SELECT * FROM OccupiedSeats WHERE flightId = ?;");
			INSERT_INTO_OCCUPIED_SEATS = session.prepare(
					"INSERT INTO OccupiedSeats (flightId, seat, customer, czas) VALUES (?, ?, ?, dateof(now()) );");
			DELETE_FROM_OCCUPIED_SEATS = session.prepare(
					"DELETE FROM OccupiedSeats WHERE flightId = ? AND seat = ?;");
					
			SELECT_FROM_OCCUPIED_SEATS_BY_COUNT = session.prepare(
					"SELECT COUNT(seat) AS dlacount FROM OccupiedSeats WHERE flightId = ? AND seat = ?;");
					
			SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHTID_AND_SEAT = session.prepare(
					"SELECT * FROM OccupiedSeats WHERE flightId = ? AND seat = ?;");
					
					
					

			SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER_AND_FLIGHT = session.prepare(
					"SELECT * FROM CustomersReservations WHERE customer = ? AND flightId = ?;");
			SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER = session.prepare(
					"SELECT * FROM CustomersReservations WHERE customer = ?;");
			INSERT_INTO_CUSTOMERS_RESERVATIONS = session.prepare(
					"INSERT INTO CustomersReservations (customer, flightId, seat) VALUES (?, ?, ?);");
			DELETE_FROM_CUSTOMERS_RESERVATIONS = session.prepare(
					"DELETE FROM CustomersReservations WHERE customer = ? AND flightId = ? AND seat = ?;");

			INSERT_INTO_FLIGHTS = session.prepare(
					"INSERT INTO Flights (Id, Name, Origin, Destination) VALUES (?, ?, ?, ?);");
			SELECT_FROM_FLIGHTS = session.prepare(
					"SELECT * FROM Flights;");
			SELECT_FROM_AIRPLANES = session.prepare(
					"SELECT * FROM Airplanes;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}




	public Flight getFlight(int filghtId) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_FREE_SEATS_BY_FLIGHT);
		bs.bind(filghtId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		Flight flight = new Flight();
		for (Row row : rs) {
			int rSeat = row.getInt("seat");
			flight.getFreeSeats().add(rSeat);
		}

		bs = new BoundStatement(SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHT);
		bs.bind(filghtId);

		rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int rSeat = row.getInt("seat");
			String rCustomer = row.getString("customer");
			flight.getOccupiedSeats().put(rSeat, rCustomer);
		}

		return flight;
	}








	public boolean bookSeats(int flightId, String customer, int count) throws BackendException {
		StringBuilder builder = new StringBuilder();
		


		int processed = 0;

		BoundStatement bs = new BoundStatement(INSERT_INTO_OCCUPIED_SEATS);
		//bs = new BoundStatement(INSERT_INTO_OCCUPIED_SEATS);
		bs.bind(flightId, count, customer );
		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		//----------
		//Thread.sleep(4000);
		//TimeUnit.SECONDS.sleep(5);
   try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(1000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
		//------------
		int ilewybranych;
		
		BoundStatement bs1 = new BoundStatement(SELECT_FROM_OCCUPIED_SEATS_BY_COUNT);   //ile customerow wybralo ten sam numer
		bs1.bind(flightId, count);  // count - wybrany numer siedzenia
		try {
			session.execute(bs1);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		int ile_wybranych = bs1.getInt("seat");

    // SELECT WOLNYCH MIEJSC
 		
		ResultSet rs2 = null;
		
		BoundStatement bs2 = new BoundStatement(SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHTID_AND_SEAT);
		bs2.bind(flightId, count);  // count - wybrany numer siedzenia
		try {
			rs2 = session.execute(bs2);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		List<String> lista = new ArrayList<String>(); //lista pusta
		
		for(Row row : rs2) {
            lista.add( row.getString("customer") );
        }
		
		System.out.println("customer = " + customer + "  wybral siedzenie numer" + count);
		
		System.out.println("pierwszy z listy = " + lista.get(0) );
		System.out.println("drugi z listy = " + lista.get(1) );
		System.out.println("trzeci z listy = " + lista.get(2) );
		
		//if( customer == lista.get(0) ){
		if( customer.equals(lista.get(0)) ){
		  System.out.println("pierwszy z listy wygral= " + lista.get(0) );
		  
		  // wygralem rezerwacje i wpisuje sie na liste rezerwacji
		  BoundStatement bs3 = new BoundStatement(INSERT_INTO_CUSTOMERS_RESERVATIONS);
        bs3 = new BoundStatement(INSERT_INTO_CUSTOMERS_RESERVATIONS);
			  bs3.bind(customer, flightId, count);
			try {
				session.execute(bs3);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		  
		// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
		  
		  
		  
		  // teraz trzeba wygrane wylosowane miejsce usunac z tebeli occupied
		  BoundStatement bs4 = new BoundStatement(DELETE_FROM_FREE_SEATS);
		  bs4 = new BoundStatement(DELETE_FROM_FREE_SEATS);
		  bs4.bind(flightId, count);
		try {
			session.execute(bs4);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		  
		  
		  
		}
		else {    // 2 tura
		
		
        		// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
		  // 5 sekund przerwy
		
		
		
		  System.out.println("NIE UDALO SIE : " + customer + "- kolejna runda" );
		 
		 ResultSet rs5 = null; 
		  BoundStatement bs5 = new BoundStatement(SELECT_FROM_FREE_SEATS_BY_FLIGHT);
		  bs5 = new BoundStatement(SELECT_FROM_FREE_SEATS_BY_FLIGHT);
		bs5.bind(flightId);  // count - wybrany numer siedzenia
		try {
			rs5 = session.execute(bs5);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		List<Integer> lista2tura = new ArrayList<>(); //lista pusta
		
		for(Row row : rs5) {
            lista2tura.add( row.getInt("seat") );
        }
	  
	  
	          		// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
		  // 5 sekund przerwy
	/*  
	  System.out.println("pierwszy z listy 2 tura = " + lista2tura.get(0).toString() );
		System.out.println("drugi z listy 2 tura = " + lista2tura.get(1) );
		System.out.println("trzeci z listy 2 tura = " + lista2tura.get(2)  );
		System.out.println("4 z listy 2 tura = " + lista2tura.get(3)  );
		System.out.println("5 z listy 2 tura = " + lista2tura.get(4).toString() );
		   //z tabeli freeseats odczytujemy wolne miejsca i kolejna proba wylosowania z wolnych miejsc
	*/	
		int lista2turarozmiar = lista2tura.size();
		//System.out.println("ROZMIAR 2 tura = " + lista2tura.size() );
		
		 
    //Random random2tura = new Random(); 
    int losowaniemiejsca2tura = new Random().nextInt(lista2turarozmiar); 
		
		//System.out.println("2 TURA WYLOSOWANO: " + losowaniemiejsca2tura);
		
		// wylosowano miejsce z listy miejsc - pobranie okreslonego miejsca z listy miejsc
		
		int wybranemiejsce2tura = lista2tura.get(losowaniemiejsca2tura);
		
		//System.out.println("2 TURA WYLOSOWANO MIEJSCE: " + wybranemiejsce2tura);
		
		// INSERT do tabeli wybranego miejsca
		
		BoundStatement bs6 = new BoundStatement(INSERT_INTO_OCCUPIED_SEATS);
		//bs = new BoundStatement(INSERT_INTO_OCCUPIED_SEATS);
		bs6.bind(flightId, wybranemiejsce2tura, customer );
		try {
			session.execute(bs6);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		
				// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
   
		// sprawdzenie kto jeszcze wybral te samo miejsce
		ResultSet rs7 = null;
		
		BoundStatement bs7 = new BoundStatement(SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHTID_AND_SEAT);
		bs7.bind(flightId, wybranemiejsce2tura);  // count - wybrany numer siedzenia
		try {
			rs7 = session.execute(bs7);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		
		
		
		
		List<String> lista2turawybranychmiejsc = new ArrayList<String>(); //lista pusta
		
		for(Row row : rs7) {
            lista2turawybranychmiejsc.add( row.getString("customer") );
        }
	/*	
		System.out.println("customer = " + customer + "  wybral siedzenie numer" + count);
		System.out.println("pierwszy z listy = " + lista.get(0) );
		System.out.println("drugi z listy = " + lista.get(1) );
		System.out.println("trzeci z listy = " + lista.get(2) );
	*/	
		//if( customer == lista.get(0) ){
		if( customer.equals(lista2turawybranychmiejsc.get(0)) ) {
		  System.out.println("2 TURA pierwszy z listy wygral= " + lista2turawybranychmiejsc.get(0) );
		
		
		   // wygralem rezerwacje i wpisuje sie na liste rezerwacji
		  BoundStatement bs8 = new BoundStatement(INSERT_INTO_CUSTOMERS_RESERVATIONS);
        bs8 = new BoundStatement(INSERT_INTO_CUSTOMERS_RESERVATIONS);
			  bs8.bind(customer, flightId, wybranemiejsce2tura);  // wybrane miejsce w 2 turze
			try {
				session.execute(bs8);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
		
			  
		// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
		  
		  
		  
		  // teraz trzeba wygrane wylosowane miejsce usunac z tebeli occupied
		  BoundStatement bs9 = new BoundStatement(DELETE_FROM_FREE_SEATS);
		  bs9 = new BoundStatement(DELETE_FROM_FREE_SEATS);
		  bs9.bind(flightId, wybranemiejsce2tura);    //usuniecie wybranego miejsca
		try {
			session.execute(bs9);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
			          		// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
		  // 5 sekund przerwy
		  
		  
				
		} else {  // jesli nie to 3 tura
		
		     //w 3 turze losuje czy chce kontynuowac
		     
		     int czybioreudzialw3rundzie = new Random().nextInt(2);
		     
		     if( czybioreudzialw3rundzie == 0 ) {
		        //  ZREZYGNOWALEM
		        
		       System.out.println("ZREZYGNOWALEM Z 3 TURY");
		       
		      
		     } 
		     
		      if( czybioreudzialw3rundzie == 1 ) {  // JESZCZE RAZ 3 TURA
		     
		     System.out.println(" 3 TURA");   // pobranie ile jest wolnych miejsc i wylosowanie miejsca
		      
		       ResultSet rs10 = null; 
		  BoundStatement bs10 = new BoundStatement(SELECT_FROM_FREE_SEATS_BY_FLIGHT);
		  bs10 = new BoundStatement(SELECT_FROM_FREE_SEATS_BY_FLIGHT);
		bs10.bind(flightId);  // count - wybrany numer siedzenia
		try {
			rs10 = session.execute(bs10);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		List<Integer> lista3tura = new ArrayList<>(); //lista pusta
		
		for(Row row : rs10) {
            lista3tura.add( row.getInt("seat") );
        }
	  
	  

		      
		      
		int lista3turarozmiar = lista3tura.size();
		System.out.println("ROZMIAR 3 tura = " + lista3tura.size() );
		
		 
    //Random random3tura = new Random(); 
    int losowaniemiejsca3tura = new Random().nextInt(lista3turarozmiar);   // wylosowanie numeru na liscie
		
		System.out.println("3 TURA WYLOSOWANO: " + losowaniemiejsca3tura);
		
		// wylosowano miejsce z listy miejsc - pobranie okreslonego miejsca z listy miejsc
		
		int wybranemiejsce3tura = lista3tura.get(losowaniemiejsca3tura);   // pobranie okreslonego miejsca z listy
		
		System.out.println("3 TURA WYLOSOWANO MIEJSCE: " + wybranemiejsce3tura);
		
		// INSERT do tabeli wybranego miejsca      
		      
		      
	 	      // INSERT do tabeli wybranego miejsca
		
		BoundStatement bs11 = new BoundStatement(INSERT_INTO_OCCUPIED_SEATS);
		//bs = new BoundStatement(INSERT_INTO_OCCUPIED_SEATS);
		bs11.bind(flightId, wybranemiejsce3tura, customer );
		try {
			session.execute(bs11);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		
				// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
   
    // sprawdzenie kto jeszcze wybral te samo miejsce
		ResultSet rs12 = null;
		
		BoundStatement bs12 = new BoundStatement(SELECT_FROM_OCCUPIED_SEATS_BY_FLIGHTID_AND_SEAT);
		bs12.bind(flightId, wybranemiejsce3tura);  // count - wybrany numer siedzenia
		try {
			rs12 = session.execute(bs12);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		
		List<String> lista3turawybranychmiejsc = new ArrayList<String>(); //lista pusta
		
		for(Row row : rs12) {
            lista2turawybranychmiejsc.add( row.getString("customer") );
        }
		      
		  // jesli jestem na 1 miejscu to wybralem
		  
		  		if( customer.equals(lista3turawybranychmiejsc.get(0)) ) {
		  System.out.println("3 TURA pierwszy z listy wygral= " + lista3turawybranychmiejsc.get(0) );
		
		
		   // wygralem rezerwacje i wpisuje sie na liste rezerwacji
		  BoundStatement bs13 = new BoundStatement(INSERT_INTO_CUSTOMERS_RESERVATIONS);
        bs13 = new BoundStatement(INSERT_INTO_CUSTOMERS_RESERVATIONS);
			  bs13.bind(customer, flightId, wybranemiejsce3tura);  // wybrane miejsce w 3 turze
			try {
				session.execute(bs13);
			} catch (Exception e) {
				throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
			}
	   	 
		 		// 5 sekund przerwy  
		  try {
     for(int n = 5; n > 0; n--) {
     System.out.println(n);
     Thread.sleep(5000);
     }
    } catch (InterruptedException e) {
      System.out.println("Przerwanie watku");
   }
   
   	  // teraz trzeba wygrane wylosowane miejsce usunac z tebeli occupied
		  BoundStatement bs14 = new BoundStatement(DELETE_FROM_FREE_SEATS);
		  bs14 = new BoundStatement(DELETE_FROM_FREE_SEATS);
		  bs14.bind(flightId, wybranemiejsce3tura);    //usuniecie wybranego miejsca
		 try {
			 session.execute(bs14);
		 } catch (Exception e) {
			 throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		 }
		
		
		
		
		}
		      
		      
		      
		      
		     
		     }  // koniec else 3 TURA
		     
		
		}
		
		
		} // koniec else 2 tura
		
		
		//String odczytanycustomer = bs2.getString("customer");
		//		 if( odczytanycustomer == customer){
		//    System.out.println("Odczany pierwszy" );

    


		return true;
	}












	public List<SeatID> getAllCustomerReservations(String customer) throws BackendException {
        BoundStatement bs = new BoundStatement(
                SELECT_FROM_CUSTOMERS_RESERVATIONS_BY_CUSTOMER
        );
        bs.bind(customer);

        ResultSet rs = null;

        try {
            rs = session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }

        List<SeatID> reservations = new ArrayList<>();
        for(Row row : rs) {
            reservations.add(new SeatID(row.getInt("flightid"), row.getInt("seat")));
        }
        return reservations;
    }


	public List<Airplane> getAvailableAirplanes() throws BackendException {
        BoundStatement bs = new BoundStatement(SELECT_FROM_AIRPLANES);
        ResultSet rs = null;
        List<Airplane> result = new ArrayList<>();
        try {
            rs = session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
        for (Row row : rs){
            result.add(new Airplane(row.getString("name"), row.getInt("seatCount")));
        }
        return result;
    }

    public List<Integer> getAllFlights() throws BackendException {
        BoundStatement bs = new BoundStatement(SELECT_FROM_FLIGHTS);
        ResultSet rs = null;
        List<Integer> result = new ArrayList<>();
        try {
            rs = session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
        for (Row row : rs){
            result.add(row.getInt("Id"));
        }
        return result;
    }

	public void addFlight(int id, String name, int seatCount, String origin, String destination) throws BackendException {
        BoundStatement bs = new BoundStatement(INSERT_INTO_FLIGHTS);
        bs.bind(id, name, origin, destination);
        try {
            session.execute(bs);
        } catch(Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }

        for (int i = 0; i < seatCount; i++) {
            bs = new BoundStatement(INSERT_INTO_FREE_SEATS);
            bs.bind(id, i);

            try {
                session.execute(bs);
            } catch (Exception e) {
                throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
            }

            logger.info("Free seat" + i + " upserted");
        }
    }

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}

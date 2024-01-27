package main;

import java.sql.*;

/**
 * 
 * @author monil, dipesh, amala
 *
 *         Connecting to the postgres database using java program
 */

public class DBMSConnectivity {

	// Main method for connecting the database and writing queries
	public static void main(String args[]) throws SQLException {

		// creating a c variable and initializing it with null value
		Connection c = null;

		// creating a rs variable and initializing it with null value
		ResultSet rs = null;

		// try/catch block for catching exceptions
		try {
			// loading the driver
			Class.forName("org.postgresql.Driver");

			// connecting to the postgreSQL database
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres",
					"Mumbaiindia!1");

			// enabling statement to write SQL queries
			Statement stmt = c.createStatement();

			// creating table Product with prodid, pname and price
			stmt.executeUpdate("create table Product(" + "prodid char(5), pname char(20), price double precision)");

			// altering table Product by adding prodid as primary key
			stmt.executeUpdate("alter table Product ADD CONSTRAINT pk_product PRIMARY KEY(prodid)");

			// inserting values into prodid, pname and price
			stmt.executeUpdate(
					"insert into Product (prodid,pname,price) values('p1','tape',2.5), ('p2','tv',250), ('p3','vcr',80)");

			// creating table Depot with dep, addr and volume
			stmt.executeUpdate("create table Depot(" + "dep char(5), addr char(20), volume double precision)");

			// altering table Depot by adding dep as the primary key
			stmt.executeUpdate("alter table Depot ADD CONSTRAINT pk_depot PRIMARY KEY(dep)");

			// inserting values into dep, addr and volume
			stmt.executeUpdate(
					"insert into Depot (dep,addr,volume) values('d1','New York',9000), ('d2','Syracuse',6000), ('d4','New York',2000)");

			// creating table Stock with prod, dep and quantity
			stmt.executeUpdate("create table Stock(" + "prod char(5), dep char(5), quantity double precision)");

			// altering table Stock by adding dep as a foreign key
			stmt.executeUpdate(
					"alter table Stock ADD CONSTRAINT pk_stock FOREIGN KEY(dep) REFERENCES Depot (dep)");

			// altering table Stock by adding prod as a foreign key
			stmt.executeUpdate(
					"alter table Stock ADD CONSTRAINT pk_stockprod FOREIGN KEY(prod) REFERENCES Product (prodid)");

			// inserting values into prod, dep and quantity
			stmt.executeUpdate(
					"insert into Stock (prod,dep,quantity) values ('p1','d1',1000), ('p1','d2',-100), ('p1','d4',1200), ('p3','d1',3000), ('p3','d4',2000), ('p2','d4',1500),('p2','d1',-400),('p2','d2',2000)");

			// deleting p1 from prodid from Product and Stock table
			stmt.executeUpdate("DELETE FROM  Stock Where prod='p1'");
			stmt.executeUpdate("DELETE FROM  Product Where prodid='p1'");

			// moving to the next row
			rs.next();

			// setAutoCommit set to false - automicity
			c.setAutoCommit(false);
		} catch (Exception e) {
			// printing the stack trace of the error
			e.printStackTrace();

			// specific error
			System.err.println(e.getClass().getName() + ": " + e.getMessage());

			// exiting the program
			System.exit(0);

			// rollbacking the connection
			c.rollback();

			// closing the connection
			c.close();
		}
	}
}
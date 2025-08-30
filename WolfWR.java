import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

/**
 * WolfWR JDBC Implementation
 */
public class WolfWR {
    // link to the stored db
    static final String jdbcURL = ""; /* Removed for security */

	private static Connection connection = null;
	private static Statement statement = null;
	private static ResultSet result = null;
	
	private static final List<String> allCommands = Arrays.asList(
		    "insertStore", "updateStore", "deleteStore",
		    "insertMember", "updateMember", "deleteMember",
		    "insertStaff", "updateStaff", "deleteStaff",
		    "insertSupplier", "updateSupplier", "deleteSupplier",
		    "insertDiscount", "updateDiscount", "deleteDiscount",
		    "insertSignUp", "updateSignUp", "deleteSignUp",
		    "insertBill", "updateBill", "deleteBill",
		    "insertMerch", "updateMerch", "deleteMerch",
		    "transferItems", "calculateTransaction", "calculateReward", "updateReward",
		    "getMerchStockByStore", "getMerchStockByItem",
		    "getSalesByDay", "getSalesinRange", "getSalesGrowth",
		    "getCustGrowth", "getCustActivity", "exit"
	);


    public static void main(String[] args) {
        try {
            initializeDatabase();
            takeInput();
        }
        finally {
            closeDatabase();
        }
    }
    
    /**
     * This function will first make a connect to the database,
     * then it will create the tables needed for WolfWR (if they already exist it will drop and recreate them)
     */
    private static void initializeDatabase() {
        try {
            connectToDatabase();
        
            createSchema();

            initializeDemoData();
            
            //System.out.println("Database schema created successfully.");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create and connect the connection to the MariaDB.
     */
    private static void connectToDatabase() throws ClassNotFoundException, SQLException {

		Class.forName("org.mariadb.jdbc.Driver");
		String user = ""; /* Removed for security */
		String password = ""; /* Removed for security */

		connection = DriverManager.getConnection(jdbcURL, user, password);
		statement = connection.createStatement();

		try {
			// Drop existing tables safely, ignoring dependency errors
            statement.execute("SET FOREIGN_KEY_CHECKS = 0");
            statement.executeUpdate("DROP TABLE IF EXISTS Transfers");
            statement.executeUpdate("DROP TABLE IF EXISTS Discounts");
            statement.executeUpdate("DROP TABLE IF EXISTS Rewards");
            statement.executeUpdate("DROP TABLE IF EXISTS Bills");
            statement.executeUpdate("DROP TABLE IF EXISTS Merchandise");
            statement.executeUpdate("DROP TABLE IF EXISTS Suppliers");
            statement.executeUpdate("DROP TABLE IF EXISTS Transactions");
            statement.executeUpdate("DROP TABLE IF EXISTS ClubMembers");
            statement.executeUpdate("DROP TABLE IF EXISTS StaffMembers");
            statement.executeUpdate("DROP TABLE IF EXISTS Stores");
            statement.executeUpdate("DROP TABLE IF EXISTS SignUps");
            statement.execute("SET FOREIGN_KEY_CHECKS = 1");

		} catch (SQLException e) {
            // If tables don't exist, then we are good to go.
		}
	}

    /**
     * Define schema and create all needed tables
     */
    private static void createSchema() throws SQLException {

        // We Split the data into different tables which followed our decomposition of relations from the original ER diagram.
        // To make it scalable and have seperation of responsibilities we make it so no data is duplicated and only tables needing 
        // the relevent data has it.

        // Composite primary keys in tables like SignUps and Transfers are used to prevent duplication and have unique identification.

        // We use ON DELETE CASCADE when if an entity is deleted we want all references to be removed, for instance if a store is removed all information associated to it is as well.
        // We use SET NULL to preserve historical data for potential audit trails needed in the future.

        // We have many foreign keys to model many-to-1 and many-to-many relations.

        // Create Stores table
        String createStores = "CREATE TABLE Stores ( " +
                "storeID INT PRIMARY KEY, " +
                "storeNum VARCHAR(32) NOT NULL, " +
                "storeAddr VARCHAR(256) NOT NULL" +
                ")";
        statement.executeUpdate(createStores);

        // Create StaffMembers table
        String createStaffMembers = "CREATE TABLE StaffMembers ( " +
                "staffID INT PRIMARY KEY, " +
                "Name VARCHAR(128) NOT NULL, " +
                "Age INT NOT NULL, " +
                "homeAddr VARCHAR(256) NOT NULL, " +
                "employmentTime VARCHAR(32) NOT NULL, " +
                "jobTitle VARCHAR(32) NOT NULL, " +
                "staffNum VARCHAR(15) NOT NULL, " +
                "staffEmail VARCHAR(128) NOT NULL, " +
                "storeID INT NOT NULL, " +
                "FOREIGN KEY (storeID) REFERENCES Stores(storeID)" +
                ")";
        statement.executeUpdate(createStaffMembers);

        // Alter Stores to add managerID with FK to StaffMembers
        String alterStores = "ALTER TABLE Stores ADD managerID INT, " +
                "ADD CONSTRAINT FK_Stores_Manager FOREIGN KEY (managerID) REFERENCES StaffMembers(staffID) ON DELETE SET NULL";
        statement.executeUpdate(alterStores);

        // Create ClubMembers table
        String createClubMembers = "CREATE TABLE ClubMembers ( " +
                "customerID INT PRIMARY KEY, " +
                "firstName VARCHAR(64) NOT NULL, " +
                "lastName VARCHAR(64) NOT NULL, " +
                "membershipLevel VARCHAR(32) NOT NULL, " +
                "custEmail VARCHAR(128) NOT NULL, " +
                "custPhone VARCHAR(15) NOT NULL, " +
                "custAddr VARCHAR(256) NOT NULL, " +
                "custStatus VARCHAR(32) NOT NULL CHECK (custStatus IN ('Active', 'Inactive'))" +
                ")";
        statement.executeUpdate(createClubMembers);

        // Create SignUps table
        String createSignUps = "CREATE TABLE SignUps ( " +
                "storeID INT NOT NULL, " +
                "customerID INT NOT NULL, " +
                "signUpDate DATE NOT NULL, " +
                "staffID INT NOT NULL, " +
                "PRIMARY KEY (storeID, customerID), " +
                "FOREIGN KEY (storeID) REFERENCES Stores(storeID), " +
                "FOREIGN KEY (customerID) REFERENCES ClubMembers(customerID), " +
                "FOREIGN KEY (staffID) REFERENCES StaffMembers(staffID)" +
                ")";
        statement.executeUpdate(createSignUps);

        // Create Transactions table
        String createTransactions = "CREATE TABLE Transactions ( " +
                "transactionID INT PRIMARY KEY, " +
                "purchaseDate DATE NOT NULL, " +
                "totalPrice DECIMAL(10,2), " +
                "customerID INT, " +
                "staffID INT, " +
                "storeID INT, " +
                "productList VARCHAR(128) NOT NULL," +
                "FOREIGN KEY (customerID) REFERENCES ClubMembers(customerID) ON DELETE SET NULL, " +
                "FOREIGN KEY (staffID) REFERENCES StaffMembers(staffID) ON DELETE SET NULL, " +
                "FOREIGN KEY (storeID) REFERENCES Stores(storeID) ON DELETE SET NULL" +
                ")";
        statement.executeUpdate(createTransactions);

        // Create Suppliers table
        String createSuppliers = "CREATE TABLE Suppliers ( " +
                "supplierID INT PRIMARY KEY, " +
                "supplierName VARCHAR(128) NOT NULL, " +
                "supplierNum VARCHAR(15) NOT NULL UNIQUE, " +
                "supplierEmail VARCHAR(128) NOT NULL UNIQUE, " +
                "location VARCHAR(256) NOT NULL" +
                ")";
        statement.executeUpdate(createSuppliers);

        // Create Merchandise table
        String createMerchandise = "CREATE TABLE Merchandise ( " +
                "storeID INT NOT NULL, " +
                "productID INT NOT NULL, " +
                "productName VARCHAR(128) NOT NULL, " +
                "stockQuantity INT NOT NULL CHECK (stockQuantity >= 0), " +
                "buyPrice DECIMAL(10,2) NOT NULL CHECK (buyPrice >= 0), " +
                "marketPrice DECIMAL(10,2) NOT NULL CHECK (marketPrice >= 0), " +
                "productionDate DATE NOT NULL, " +
                "expirationDate DATE, " +
                "supplierID INT NOT NULL, " +
                "FOREIGN KEY (storeID) REFERENCES Stores(storeID) ON DELETE CASCADE, " +
                "FOREIGN KEY (supplierID) REFERENCES Suppliers(supplierID) ON DELETE CASCADE, " +
                "PRIMARY KEY (storeID, productID)" +
                ")";
        statement.executeUpdate(createMerchandise);

        // Create Bills table
        String createBills = "CREATE TABLE Bills ( " +
                "billID INT PRIMARY KEY, " +
                "amountOwed DECIMAL(10,2) NOT NULL, " +
                "status VARCHAR(32) NOT NULL CHECK (status IN ('paid', 'unpaid')), " +
                "staffID INT NOT NULL, " +
                "supplierID INT NOT NULL, " +
                "FOREIGN KEY (staffID) REFERENCES StaffMembers(staffID), " +
                "FOREIGN KEY (supplierID) REFERENCES Suppliers(supplierID)" +
                ")";
        statement.executeUpdate(createBills);

        // Create Rewards table
        String createRewards = "CREATE TABLE Rewards ( " +
                "rewardID INT PRIMARY KEY, " +
                "checkAmountOwed DECIMAL(10,2) NOT NULL, " +
                "staffID INT NOT NULL, " +
                "customerID INT NOT NULL, " +
                "FOREIGN KEY (staffID) REFERENCES StaffMembers(staffID), " +
                "FOREIGN KEY (customerID) REFERENCES ClubMembers(customerID)" +
                ")";
        statement.executeUpdate(createRewards);

        // Create Discounts table
        String createDiscounts = "CREATE TABLE Discounts ( " +
                "discountID INT PRIMARY KEY, " +
                "productID INT NOT NULL, " +
                "storeID INT NOT NULL, " +
                "discountStartDate DATE NOT NULL, " +
                "discountEndDate DATE NOT NULL, " +
                "promotion DECIMAL(10,2) NOT NULL, " +
                "FOREIGN KEY (storeID, productID) REFERENCES Merchandise(storeID, productID) ON DELETE CASCADE" +
                ")";
        statement.executeUpdate(createDiscounts);

        // Create Transfers table
        String createTransfers = "CREATE TABLE Transfers ( " +
                "store1ID INT NOT NULL, " +
                "store2ID INT NOT NULL, " +
                "product1ID INT NOT NULL, " +
                "product2ID INT NOT NULL, " +
                "transferDate DATE NOT NULL, " +
                "staffID INT NOT NULL, " +
                "PRIMARY KEY (store1ID, store2ID, product1ID, product2ID), " +
//                "FOREIGN KEY (store1ID, product1ID) REFERENCES Merchandise(storeID, productID), " +
                "FOREIGN KEY (store2ID, product2ID) REFERENCES Merchandise(storeID, productID) ON DELETE CASCADE, " +
                "FOREIGN KEY (staffID) REFERENCES StaffMembers(staffID) ON DELETE CASCADE" +
                ")";
        statement.executeUpdate(createTransfers);
    }

    /**
     * Used to fill the tables with the data needed for the demo
     */
    private static void initializeDemoData() throws SQLException {

        // Stores with managerID = NULL    
            enterStoreInfo(1001, null, "1021 Main Campus Dr, Raleigh, NC 27606", "919-478-9124");
        
            enterStoreInfo(1002, null, "851 Partners Way, Raleigh, NC 27606", "919-592-9621");
        
        //enter Staff from Demo Data
            enterStaffInfo(201, 1001, "Alice Johnson", "34", "111 Wolf Street, Raleigh, NC 27606", 
                    "Manager", "9194285357", "alice.johson@gmail.com", "5 years");
            
            enterStaffInfo(202, 1002, "Bob Smith", "29", "222 Fox Ave, Durham, NC 27701", 
                    "Assistant Manager", "9841482375", "bob.smith@hotmail.com", "3 years");
            
            enterStaffInfo(203, 1001, "Charlie Davis", "40", "333 Bear Rd, Greensboro NC 27282", 
                    "Cashier", "9194856193", "charlie.davis@gmail.com", "7 years");
            
            enterStaffInfo(204, 1002, "David Lee", "45", "444 Eagle Drive, Raleigh, NC 27606", 
                    "Warehouse Checker", "9847028471", "david.lee@yahoo.com", "10 years");
            
            enterStaffInfo(205, 1001, "Emma White", "30", "555 Deer Ln, Durham, NC 27560", 
                    "Billing Staff", "9198247184", "emma.white@gmail.com", "4 years");
            
            enterStaffInfo(206, 1002, "Frank Harris", "38", "666 Owl Ct, Raleigh, NC 27610", 
                    "Billing Staff", "919428535", "frank.harris@gmail.com", "6 years");
            
            enterStaffInfo(207, 1001, "Isla Scott", "33", "777 Lynx Rd, Raleigh, NC 27612", 
                    "Warehouse Checker", "9841298427", "isla.scott@gmail.com", "2 years");
            
            enterStaffInfo(208, 1002, "Jack Lewis", "41", "888 Falcon St, Greensboro, NC 27377", 
                    "Cashier", "9194183951", "jack.lewis@gmail.com", "3 years");
            
            //Enter Suppliers from Demo Data
        
            enterSupplierInfo(401, "Fresh Farms Ltd.", "9194248251", "contact@freshfarms.com", "123 Greenway Blvd, Raleigh, NC 27615");
            
            enterSupplierInfo(402, "Organic Good Inc.", "9841384298", "info@organicgoods.com", "456 Healthy Rd, Raleigh, NC 27606");
            
            //Enter inventory from Demo Data
            
            insertInventory(1002, 301, "Organic Apples", 120, 1.5, 2.0, "2025-04-12", "2025-04-20", 401);
            
            insertInventory(1002, 302, "Whole Grain Bread", 80, 2.0, 3.5, "2025-04-10", "2025-04-15", 401);
            
            insertInventory(1002, 303, "Almond Milk", 150, 3.5, 4.0, "2025-04-15", "2025-04-30", 401);
            
            insertInventory(1002, 304, "Brown Rice", 200, 2.8, 3.5, "2025-04-12", "2026-04-20", 402);
            
            insertInventory(1002, 305, "Olive Oil", 90, 5.0, 7.0, "2025-04-04", "2027-04-20", 402);
            
            insertInventory(1002, 306, "Whole Chicken", 75, 10.0, 13.0, "2025-04-12", "2025-05-12", 402);
            
            insertInventory(1002, 307, "Cheddar Cheese", 60, 3.0, 4.2, "2025-04-12", "2025-10-12", 402);
            
            insertInventory(1002, 308, "Dark Chocolate", 50, 2.5, 3.5, "2025-04-12", "2026-06-20", 402);
            
            //Enter discount information from Demo Data
            
            enterDiscountInfo(601, 306, 1002, 10.0, "2024-04-10", "2024-05-10");
            
            enterDiscountInfo(602, 303, 1002, 20.0, "2023-02-12", "2023-02-19");
            
            //Enter club member information from Demo Data
            
            enterMemberInfo(501, "John", "Doe", "Gold", "john.doe@gmail.com", "9194285314", "12 Elm St, Raleigh, NC 27607", "Active");
            
            enterMemberInfo(502, "Emily", "Smith", "Silver", "emily.smith@gmail.com", "9844235314", "34 Oak Ave, Raleigh, NC 27607", "Inactive");
            
            enterMemberInfo(503, "Michael", "Brown", "Platinum", "michael.brown@gmail.com", "9194820931", "56 Pine Rd, Raleigh, NC 27607", "Active");
            
            enterMemberInfo(504, "Sarah", "Johnson", "Gold", "sarah.johnson@gmail.com", "9841298435", "78 Maple St, Raleigh, NC 27607", "Active");
            
            enterMemberInfo(505, "David", "Williams", "Silver", "david.williams@gmail.com", "9194829424", "90 Birch Ln, Raleigh, NC 27607", "Inactive");
            
            enterMemberInfo(506, "Anna", "Miller", "Platinum", "anna.miller@gmail.com", "9194829424", "101 Oak Ct, Raleigh, NC 27607", "Active");
            
            //Enter Sign Up Information
            
            enterSignUp(1001, 501, "2024-01-31", 201);
            
            enterSignUp(1001, 502, "2022-02-28", 201);
            
            enterSignUp(1002, 503, "2020-03-22", 201);
            
            enterSignUp(1002, 504, "2023-03-15", 201);
            
            enterSignUp(1002, 505, "2024-08-23", 201);
            
            enterSignUp(1002, 506, "2025-02-10", 201);
            
            //Insert Transactions
            
            insertTransaction(701, "2024-02-10", 45.0, 502, 203, 1002, "Organic Apples, Whole Grain Bread");
            
            insertTransaction(702, "2024-09-12", 60.75, 502, 208, 1002, "Almond Milk, Brown Rice, Olive Oil");
            
            insertTransaction(703, "2024-09-23", 78.9, 502, 208, 1002, "Dark Chocolate, Olive Oil, Almond Milk");
            
            insertTransaction(704, "2024-07-23", 32.5, 504, 203, 1002, "Whole Chicken");
            
            //update store 1 with proper manager field
            
            updateStoreInfo(1001, 201, "1021 Main Campus Dr, Raleigh, NC 27606", "919-478-9124");
            
        // Add as many additional inserts as you need here to match your Word doc data.
        System.out.println("Demo data successfully loaded.");
    }
    
    // Method that takes input from user and executes functions with them.
    // This is our simple way for users to interact with the db without a GUI.
    private static void takeInput() {
    	Scanner input = new Scanner(System.in);
        Set<String> allowedCommands = new HashSet<>();
        int viewSelection = -1;

        // Loop until a valid view is selected
        while (viewSelection < 1 || viewSelection > 5) {
            System.out.println("Select your view:");
            System.out.println("1. Registration office");
            System.out.println("2. Admin");
            System.out.println("3. Warehouse Operator");
            System.out.println("4. Billing Staff");
            System.out.println("5. Cashier");

            if (input.hasNextInt()) {
                viewSelection = input.nextInt();
                input.nextLine(); // consume newline
            } else {
                input.nextLine(); // consume invalid input
                System.out.println("Invalid input. Please enter a number between 1 and 5.");
                continue;
            }

            switch (viewSelection) {
                case 1:
                    allowedCommands.addAll(Arrays.asList("insertMember", "deleteMember", "insertSignUp", "deleteSignUp", "exit"));
                    break;
                case 2:
                	allowedCommands.addAll(allCommands);
                    break;
                case 3:
                    allowedCommands.addAll(Arrays.asList("insertMerch", "updateMerch", "deleteMerch", "transferItems", "exit"));
                    break;
                case 4:
                    allowedCommands.addAll(Arrays.asList(
                        "getMerchStockByStore", "getMerchStockByItem", "getSalesByDay", "getSalesinRange", 
                        "getSalesGrowth", "getCustGrowth", "getCustActivity", "insertBill", "updateBill", "deleteBill", "calculateReward", "updateReward", "exit"));
                    break;
             case 5:
                    allowedCommands.addAll(Arrays.asList("calculateTransaction", "exit"));
                    break;
                default:
                    System.out.println("Invalid selection. Please try again.");
                    viewSelection = -1; // reset to loop again
            }
        }
        
        // Print allowed commands before entering command loop
        System.out.println("\nCommands you can use are: " + String.join(", ", allowedCommands));

        // Begin command loop
    	boolean tester= true;
    	while(tester) {
            System.out.println("\nEnter a command (type 'exit' to quit):");
        	String command = input.nextLine();

        	if (!allowedCommands.contains(command)) {
        	    System.out.println("Command not allowed in this view.");
        	    continue;
        	}

        	switch (command) {
        	case "exit":
        		System.out.println("exiting functions");
        		tester = false;
        		break;
        	case "insertStore":
        		System.out.println("Please enter storeID:");
        		int storeID = input.nextInt();
        		System.out.println("Please enter managerID:");
        		int managerID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter store Address");
        		String storeAddress = input.nextLine();
        		System.out.println("Please enter store phone number");
        		String storePhone = input.nextLine();
        		try {
        			if(managerID < 0) {
        				enterStoreInfo(storeID, null, storeAddress, storePhone);
        			}
        			else {
        				enterStoreInfo(storeID, managerID, storeAddress, storePhone);
        			}
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateStore":
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		System.out.println("Please enter managerID:");
        		managerID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter store Address");
        		storeAddress = input.nextLine();
        		System.out.println("Please enter store phone number");
        		storePhone = input.nextLine();
        		try {
        			updateStoreInfo(storeID, managerID, storeAddress, storePhone);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteStore":
        		System.out.println("Please enter storeID");
        		storeID = input.nextInt();
        		try {
        			deleteStoreInfo(storeID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		input.nextLine();
        		break;
        	case "insertMember":
        		System.out.println("Please enter memberID:");
        		int memberID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter first name:");
        		String firstName = input.nextLine();
        		System.out.println("Please enter last name:");
        		String lastName = input.nextLine();
        		System.out.println("Please enter membershipLevel:");
        		String membershipLevel = input.nextLine();
        		System.out.println("Please enter home Address");
        		String homeAddress = input.nextLine();
        		System.out.println("Please enter phone number");
        		String phone = input.nextLine();
        		System.out.println("Please enter email");
        		String email = input.nextLine();
        		System.out.println("Please enter account Status");
        		String activeStatus = input.nextLine();
        		try {
        			enterMemberInfo(memberID, firstName, lastName, membershipLevel, email, phone, homeAddress, activeStatus);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateMember":
        		System.out.println("Please enter memberID:");
        		memberID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter first name:");
        		firstName = input.nextLine();
        		System.out.println("Please enter last name:");
        		lastName = input.nextLine();
        		System.out.println("Please enter membershipLevel:");
        		membershipLevel = input.nextLine();
        		System.out.println("Please enter home Address");
        		homeAddress = input.nextLine();
        		System.out.println("Please enter phone number");
        		phone = input.nextLine();
        		System.out.println("Please enter email");
        		email = input.nextLine();
        		System.out.println("Please enter account Status");
        		activeStatus = input.nextLine();
        		try {
        			updateMemberInfo(memberID, firstName, lastName, membershipLevel, email, phone, homeAddress, activeStatus);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteMember":
        		System.out.println("Please enter memberID:");
        		memberID = input.nextInt();
        		input.nextLine();
        		try {
        			deleteMemberInfo(memberID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "insertStaff":
        		System.out.println("Please enter staffID:");
        		int staffID = input.nextInt();
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter name:");
        		String name = input.nextLine();
        		System.out.println("Please enter age:");
        		String age = input.nextLine();
        		System.out.println("Please enter home Address");
        		homeAddress = input.nextLine();
        		System.out.println("Please enter phone number");
        		String phoneNumber = input.nextLine();
        		System.out.println("Please enter email");
        		String emailAddress = input.nextLine();
        		System.out.println("Please enter job title:");
        		String jobTitle = input.nextLine();
        		System.out.println("Please enter Time of Employment:");
        		String timeOfEmployment = input.nextLine();
        		try {
        			enterStaffInfo(staffID, storeID, name, age, homeAddress, jobTitle, phoneNumber, emailAddress, timeOfEmployment);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateStaff":
        		System.out.println("Please enter staffID:");
        		staffID = input.nextInt();
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter name:");
        		name = input.nextLine();
        		System.out.println("Please enter age:");
        		age = input.nextLine();
        		System.out.println("Please enter home Address");
        		homeAddress = input.nextLine();
        		System.out.println("Please enter phone number");
        		phoneNumber = input.nextLine();
        		System.out.println("Please enter email");
        		emailAddress = input.nextLine();
        		System.out.println("Please enter job title:");
        		jobTitle = input.nextLine();
        		System.out.println("Please enter Time of Employment:");
        		timeOfEmployment = input.nextLine();
        		try {
        			updateStaffInfo(staffID, storeID, name, age, homeAddress, jobTitle, phoneNumber, emailAddress, timeOfEmployment);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteStaff":
        		System.out.println("Please enter staffID:");
        		staffID = input.nextInt();
        		input.nextLine();
        		try {
        			deleteStaffInfo(staffID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "insertSupplier":
        		System.out.println("Please enter supplierID:");
        		int supplierID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter name of supplier:");
        		String supplierName = input.nextLine();
        		System.out.println("Please enter phone number");
        		phone = input.nextLine();
        		System.out.println("Please enter email");
        		emailAddress = input.nextLine();
        		System.out.println("Please enter location:");
        		String location = input.nextLine();
        		try {
        			enterSupplierInfo(supplierID, supplierName, phone, emailAddress, location);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateSupplier":
        		System.out.println("Please enter supplierID:");
        		supplierID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter name of supplier:");
        		supplierName = input.nextLine();
        		System.out.println("Please enter phone number");
        		phone = input.nextLine();
        		System.out.println("Please enter email");
        		emailAddress = input.nextLine();
        		System.out.println("Please enter location:");
        		location = input.nextLine();
        		try {
        			updateSupplierInfo(supplierID, supplierName, phone, emailAddress, location);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteSupplier":
        		System.out.println("Please enter supplierID:");
        		supplierID = input.nextInt();
        		input.nextLine();
        		try {
        			deleteSupplierInfo(supplierID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "insertDiscount":
        		System.out.println("Please enter discountID");
        		int discountID = input.nextInt();
        		System.out.println("Please enter productID");
        		int productID = input.nextInt();
        		System.out.println("Please enter storeID");
        		storeID = input.nextInt();
        		System.out.println("Please enter discountDetails");
        		double discountDetails = input.nextDouble();
        		input.nextLine();
        		System.out.println("Please enter startDate");
        		String startDate = input.nextLine();
        		System.out.println("Please enter endDate");
        		String endDate = input.nextLine();
        		try {
        			enterDiscountInfo(discountID, productID, storeID, discountDetails, startDate, endDate);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateDiscount":
        		System.out.println("Please enter discountID");
        		discountID = input.nextInt();
        		System.out.println("Please enter productID");
        		productID = input.nextInt();
        		System.out.println("Please enter storeID");
        		storeID = input.nextInt();
        		System.out.println("Please enter discountDetails");
        		discountDetails = input.nextDouble();
        		input.nextLine();
        		System.out.println("Please enter startDate");
        		startDate = input.nextLine();
        		System.out.println("Please enter endDate");
        		endDate = input.nextLine();
        		try {
        			updateDiscountInfo(discountID, productID, storeID, discountDetails, startDate, endDate);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteDiscount":
        		System.out.println("Please enter discountID");
        		discountID = input.nextInt();
        		try {
        			deleteDiscountInfo(discountID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		input.nextLine();
        		break;
        	case "insertSignUp":
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		System.out.println("Please enter staffID:");
        		staffID = input.nextInt();
        		System.out.println("Please enter customerID:");
        		int custID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter sign-up date:");
        		String date = input.nextLine();
        		try {
        			enterSignUp(storeID, custID, date, staffID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateSignUp":
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		System.out.println("Please enter staffID:");
        		staffID = input.nextInt();
        		System.out.println("Please enter customerID:");
        		custID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter sign-up date:");
        		date = input.nextLine();
        		try {
        			updateSignUp(storeID, custID, date, staffID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteSignUp":
        		System.out.println("Please enter customerID:");
        		custID = input.nextInt();
        		input.nextLine();
        		try {
        			deleteSignUp(custID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "insertMerch":
        		System.out.println("Please enter productID");
        		productID = input.nextInt();
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		System.out.println("Please enter supplierID:");
        		supplierID = input.nextInt();
        		System.out.println("Please enter quantity currently in stock:");
        		int stockQuantity = input.nextInt();
        		System.out.println("Please enter buy price");
        		double buyPrice = input.nextDouble();
        		System.out.println("Please enter market price");
        		double marketPrice = input.nextDouble();
        		input.nextLine();
        		System.out.println("Please enter product name");
        		String productName = input.nextLine();
        		System.out.println("Please enter production Date");
        		String productionDate = input.nextLine();
        		System.out.println("Please enter expiration Date");
        		String expirationDate = input.nextLine();
        		try {
        			insertInventory(storeID, productID, productName, stockQuantity, buyPrice, marketPrice, productionDate, expirationDate, supplierID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateMerch":
        		System.out.println("Please enter productID");
        		productID = input.nextInt();
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		System.out.println("Please enter supplierID:");
        		supplierID = input.nextInt();
        		System.out.println("Please enter quantity currently in stock:");
        		stockQuantity = input.nextInt();
        		System.out.println("Please enter buy price");
        		buyPrice = input.nextDouble();
        		System.out.println("Please enter market price");
        		marketPrice = input.nextDouble();
        		input.nextLine();
        		System.out.println("Please enter product name");
        		productName = input.nextLine();
        		System.out.println("Please enter production Date");
        		productionDate = input.nextLine();
        		System.out.println("Please enter expiration Date");
        		expirationDate = input.nextLine();
        		try {
        			updateInventory(storeID, productID, productName, stockQuantity, buyPrice, marketPrice, productionDate, expirationDate, supplierID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteMerch":
        		System.out.println("Please enter productID");
        		productID = input.nextInt();
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		input.nextLine();
        		try {
        			deleteInventory(storeID, productID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "insertBill":
        		System.out.println("Please enter billID:");
        		int billID = input.nextInt();
        		System.out.println("Please enter staffID:");
        		staffID = input.nextInt();
        		System.out.println("Please enter supplierID:");
        		supplierID = input.nextInt();
        		System.out.println("Please enter amountOwed:");
        		Double amountOwed = input.nextDouble();
        		input.nextLine();
        		System.out.println("Please enter status (must be paid or unpaid):");
        		String status = input.nextLine();
        		try {
        			generateBill(billID, amountOwed, status, staffID, supplierID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateBill":
        		System.out.println("Please enter billID:");
        		billID = input.nextInt();
        		System.out.println("Please enter staffID:");
        		staffID = input.nextInt();
        		System.out.println("Please enter supplierID:");
        		supplierID = input.nextInt();
        		System.out.println("Please enter amountOwed:");
        		amountOwed = input.nextDouble();
        		input.nextLine();
        		System.out.println("Please enter status (must be paid or unpaid):");
        		status = input.nextLine();
        		try {
        			updateBill(billID, amountOwed, status, staffID, supplierID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "deleteBill":
        		System.out.println("Please enter billID:");
        		billID = input.nextInt();
        		input.nextLine();
        		try {
        			deleteBill(billID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "transferItems":
        		System.out.println("Please enter first productID");
        		Integer product1ID = input.nextInt();
        		System.out.println("Please enter second productID");
        		Integer product2ID = input.nextInt();
        		System.out.println("Please enter storeID to be transformed from");
        		Integer store1ID = input.nextInt();
        		System.out.println("Please enter storeID to be transformed to");
        		Integer store2ID = input.nextInt();
        		System.out.println("Please enter staffID who made the transfer");
        		staffID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter transfer Date");
        		String transferDate = input.nextLine();
        		try {
        			processTransfer(store1ID, store2ID, product1ID, product2ID, transferDate, staffID);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "getMerchStockByStore":
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		try {
        			System.out.println(getMerchStockByStore(storeID));
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		input.nextLine();
        		break;
        	case "getMerchStockByItem":
        		System.out.println("Please enter merchandise name:");
        		String itemName = input.nextLine();
        		try {
        			System.out.println(getMerchStockByItem(itemName));
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "getSalesByDay":
        		System.out.println("Please enter date:");
        		date = input.nextLine();
        		try {
        			System.out.println(calculateSalesByDay(date));
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "getSalesinRange":
        		System.out.println("Please enter start date:");
        		date = input.nextLine();
        		System.out.println("Please enter end date:");
        		endDate = input.nextLine();
        		try {
        			System.out.println(calculateSalesByYear(date, endDate));
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "getSalesGrowth":
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter start date:");
        		date = input.nextLine();
        		System.out.println("Please enter end date:");
        		endDate = input.nextLine();
        		try {
        			System.out.println(calculateSalesGrowth(storeID, date, endDate));
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "getCustGrowth":
        		System.out.println("Please enter start date:");
        		date = input.nextLine();
        		System.out.println("Please enter end date:");
        		endDate = input.nextLine();
        		try {
        			System.out.println(getCustGrowthReport(date, endDate));
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "getCustActivity":
        		System.out.println("Please enter memberID:");
        		memberID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter start date:");
        		date = input.nextLine();
        		System.out.println("Please enter end date:");
        		endDate = input.nextLine();
        		try {
        			System.out.println(getCustActivityReport(memberID, date, endDate));
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "calculateReward":
        		System.out.println("Please enter rewardID");
        		int rewardID = input.nextInt();
        		System.out.println("Please enter staffID");
        		staffID = input.nextInt();
        		System.out.println("Please enter customerID");
        		int customerID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter startDate");
        		startDate = input.nextLine();
        		System.out.println("Please enter endDate");
        		endDate = input.nextLine();
        		try {
        			 createReward(rewardID, 0.0, staffID, customerID, startDate, endDate);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "updateReward":
        		System.out.println("Please enter rewardID");
        		rewardID = input.nextInt();
        		System.out.println("Please enter staffID");
        		staffID = input.nextInt();
        		System.out.println("Please enter customerID");
        		customerID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter startDate");
        		startDate = input.nextLine();
        		System.out.println("Please enter endDate");
        		endDate = input.nextLine();
        		try {
        			 updateReward(rewardID, 0.0, staffID, customerID, startDate, endDate);
        		}
        		catch(Exception e) {
        			e.printStackTrace();
        		}
        		break;
        	case "calculateTransaction":
        		System.out.println("Please enter transactionID");
        		int transactionID = input.nextInt();
        		System.out.println("Please enter customerID:");
        		customerID = input.nextInt();
        		System.out.println("Please enter staffID:");
        		staffID = input.nextInt();
        		System.out.println("Please enter storeID:");
        		storeID = input.nextInt();
        		input.nextLine();
        		System.out.println("Please enter purchase Date");
        		String purchaseDate = input.nextLine();
        		System.out.println("Please enter list of product names separated by only commas (no spaces)");
        		String productList = input.nextLine();
        		System.out.println("Please enter list of product amounts purchased separated by only commas (no spaces)");
        		String amounts = input.nextLine();
        		try {
        			calculateTransaction(transactionID, purchaseDate, customerID, staffID, storeID, productList, amounts);
	       		}
	       		catch(Exception e) {
	       			e.printStackTrace();
	       		}
	       		break;
        	}
        	
    	}
    }

    /**
     * Close all connections to the Database to prevent DB server connectivity overload issues.
     */
    private static void closeDatabase() {
        // Close result, statement, and connection in reverse order
        try {
            if (result != null)    result.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
            System.out.println("Closed from Database");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ***********************************************************************
    // INFORMATION PROCESSING QUERIES
    // These functions handle basic CRUD (Create, Read, Update, Delete) 
    // operations for various entities in the database
    // ***********************************************************************

    // Store operations
    // Inserts a new record into the Stores table.
    public static String enterStoreInfo(int storeID, Integer managerID, String storeAddress, String phoneNumber) throws SQLException {
        String sql = "INSERT INTO Stores (storeID, storeNum, storeAddr, managerID) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, storeID);
            ps.setString(2, phoneNumber);
            ps.setString(3, storeAddress);

            if (managerID == null) {
                ps.setNull(4, java.sql.Types.INTEGER);
            } else {
                ps.setInt(4, managerID);
            }
            ps.executeUpdate();
        }
        catch (Exception e) {
            throw new SQLException("Failed to insert Store. DB says: " + e.getMessage(), e);
        }
        return "Store (ID=" + storeID + ") inserted successfully.";
    }

    // Update existing store with new parameters
    public static String updateStoreInfo(int storeID, Integer managerID, String storeAddress, String phoneNumber) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE Stores SET ");
        boolean first = true;
        if (phoneNumber != null) {
            sql.append("storeNum = ?");
            first = false;
        }
        if (storeAddress != null) {
            if (!first) sql.append(", ");
            sql.append("storeAddr = ?");
            first = false;
        }
        if (managerID != null) {
            if (!first) sql.append(", ");
            sql.append("managerID = ?");
        }
        sql.append(" WHERE storeID = ?");

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (phoneNumber != null) {
                ps.setString(index++, phoneNumber);
            }
            if (storeAddress != null) {
                ps.setString(index++, storeAddress);
            }
            if (managerID != null) {
                ps.setInt(index++, managerID);
            }
            ps.setInt(index, storeID);
            ps.executeUpdate();
        } catch (Exception e) {
        	return "Failed.";
        }
        return "Store info updated successfully.";
    }

    // Delete an existing store
    public static String deleteStoreInfo(int storeID) throws SQLException {
        String sql = "DELETE FROM Stores WHERE storeID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, storeID);
            ps.executeUpdate();
        } catch (Exception e) {
        	return "Failed.";
        }
        return "Store info deleted successfully.";
    }

    // Club Member operations
    // Insert a new customer into the ClubMembers Table
    public static String enterMemberInfo(int memberID, String firstName, String lastName, String membershipLevel, 
                                     String email, String phone, String homeAddress, String activeStatus) throws SQLException {
        String sql = "INSERT INTO ClubMembers (customerID, firstName, lastName, membershipLevel, custEmail, custPhone, custAddr, custStatus) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, memberID);
            ps.setString(2, firstName);
            ps.setString(3, lastName);
            ps.setString(4, membershipLevel);
            ps.setString(5, email);
            ps.setString(6, phone);
            ps.setString(7, homeAddress);
            ps.setString(8, activeStatus);
            ps.executeUpdate();
        } catch (Exception e) {
        	return "Failed.";
        }
        return "Member info entered successfully.";
    }

    // Update values associated to an existing Club Member
    public static String updateMemberInfo(int memberID, String firstName, String lastName, String membershipLevel, 
                                      String email, String phone, String homeAddress, String activeStatus
                                      ) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE ClubMembers SET ");
        boolean first = true;
        if (firstName != null) { sql.append("firstName = ?"); first = false; }
        if (lastName != null) { sql.append(first ? "" : ", ").append("lastName = ?"); first = false; }
        if (membershipLevel != null) { sql.append(first ? "" : ", ").append("membershipLevel = ?"); first = false; }
        if (email != null) { sql.append(first ? "" : ", ").append("custEmail = ?"); first = false; }
        if (phone != null) { sql.append(first ? "" : ", ").append("custPhone = ?"); first = false; }
        if (homeAddress != null) { sql.append(first ? "" : ", ").append("custAddr = ?"); first = false; }
        if (activeStatus != null) { sql.append(first ? "" : ", ").append("custStatus = ?"); first = false; }
//        if (staffID != null) { sql.append(first ? "" : ", ").append("staffID = ?"); first = false; }
//        if (signUpDate != null) { sql.append(first ? "" : ", ").append("signUpDate = ?"); }
        sql.append(" WHERE customerID = ?");

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (firstName != null) ps.setString(index++, firstName);
            if (lastName != null) ps.setString(index++, lastName);
            if (membershipLevel != null) ps.setString(index++, membershipLevel);
            if (email != null) ps.setString(index++, email);
            if (phone != null) ps.setString(index++, phone);
            if (homeAddress != null) ps.setString(index++, homeAddress);
            if (activeStatus != null) ps.setString(index++, activeStatus);
//            if (staffID != null) ps.setInt(index++, staffID);
//            if (signUpDate != null) ps.setDate(index++, java.sql.Date.valueOf(signUpDate));
            ps.setInt(index, memberID);
            ps.executeUpdate();
        } catch (Exception e) {
        	return "Failed";
        }
        return "Member info updated successfully.";
    }

    // Delete an existing club member from the ClubMembers table
    public static String deleteMemberInfo(int memberID) throws SQLException {
        String sql = "DELETE FROM ClubMembers WHERE customerID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, memberID);
            ps.executeUpdate();
        } catch (Exception e) {
        	return "Failed";
        }
        return "Member info deleted successfully.";
    }

    // Staff operations
    // Insert a new Staff Member into the StaffMembers Table 
    public static String enterStaffInfo(int staffID, int storeID, String name, String age, String homeAddress, 
                                    String jobTitle, String phoneNumber, String emailAddress, String timeOfEmployment) throws SQLException {
        String sql = "INSERT INTO StaffMembers (staffID, Name, Age, homeAddr, employmentTime, jobTitle, staffNum, staffEmail, storeID) " +
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, staffID);
            ps.setString(2, name);
            ps.setString(3, age);
            ps.setString(4, homeAddress);
            ps.setString(5, timeOfEmployment);
            ps.setString(6, jobTitle);
            ps.setString(7, phoneNumber);
            ps.setString(8, emailAddress);
            ps.setInt(9, storeID);
            ps.executeUpdate();
        } catch (Exception e) {
        	return "Staff entering failed.";
        }
        return "Staff info entered successfully.";
    }

    // Update values associated to an existing Staff member
    public static String updateStaffInfo(int staffID, Integer storeID, String name, String age, String homeAddress, 
                                     String jobTitle, String phoneNumber, String emailAddress, String timeOfEmployment) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE StaffMembers SET ");
        boolean first = true;
        if (name != null) { sql.append("Name = ?"); first = false; }
        if (age != null) { sql.append(first ? "" : ", ").append("Age = ?"); first = false; }
        if (homeAddress != null) { sql.append(first ? "" : ", ").append("homeAddr = ?"); first = false; }
        if (timeOfEmployment != null) { sql.append(first ? "" : ", ").append("hireDate = ?"); first = false; }
        if (jobTitle != null) { sql.append(first ? "" : ", ").append("jobTitle = ?"); first = false; }
        if (phoneNumber != null) { sql.append(first ? "" : ", ").append("staffNum = ?"); first = false; }
        if (emailAddress != null) { sql.append(first ? "" : ", ").append("staffEmail = ?"); first = false; }
        if (storeID != null) { sql.append(first ? "" : ", ").append("storeID = ?"); }
        sql.append(" WHERE staffID = ?");

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (name != null) ps.setString(index++, name);
            if (age != null) ps.setDate(index++, java.sql.Date.valueOf(age));
            if (homeAddress != null) ps.setString(index++, homeAddress);
            if (timeOfEmployment != null) ps.setDate(index++, java.sql.Date.valueOf(timeOfEmployment));
            if (jobTitle != null) ps.setString(index++, jobTitle);
            if (phoneNumber != null) ps.setString(index++, phoneNumber);
            if (emailAddress != null) ps.setString(index++, emailAddress);
            if (storeID != null) ps.setInt(index++, storeID);
            ps.setInt(index, staffID);
            ps.executeUpdate();
        }
        return "Staff info updated successfully.";
    }

    // Delete an existing staff member from the StaffMembers Table
    public static String deleteStaffInfo(int staffID) throws SQLException {
        String sql = "DELETE FROM StaffMembers WHERE staffID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, staffID);
            ps.executeUpdate();
        }
        return "Staff info deleted successfully.";
    }

    // Supplier operations
    // Add a new supplier into the Suppliers Table
    public static String enterSupplierInfo(int supplierID, String supplierName, String phone, String emailAddress, String location) throws SQLException {
        String sql = "INSERT INTO Suppliers (supplierID, supplierName, supplierNum, supplierEmail, location) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, supplierID);
            ps.setString(2, supplierName);
            ps.setString(3, phone);
            ps.setString(4, emailAddress);
            ps.setString(5, location);
            ps.executeUpdate();
        }
        return "Supplier info entered successfully.";
    }

    // Update values associated with an existing supplier
    public static String updateSupplierInfo(int supplierID, String supplierName, String phone, String emailAddress, String location) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE Suppliers SET ");
        boolean first = true;
        if (supplierName != null) { sql.append("supplierName = ?"); first = false; }
        if (phone != null) { sql.append(first ? "" : ", ").append("supplierNum = ?"); first = false; }
        if (emailAddress != null) { sql.append(first ? "" : ", ").append("supplierEmail = ?"); first = false; }
        if (location != null) { sql.append(first ? "" : ", ").append("location = ?"); }
        sql.append(" WHERE supplierID = ?");
    
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (supplierName != null) ps.setString(index++, supplierName);
            if (phone != null) ps.setString(index++, phone);
            if (emailAddress != null) ps.setString(index++, emailAddress);
            if (location != null) ps.setString(index++, location);
            ps.setInt(index, supplierID);
            ps.executeUpdate();
        }
        return "Supplier info updated successfully.";
    }

    // Delete an existing supplier from Supplier table
    public static String deleteSupplierInfo(int supplierID) throws SQLException {
        String sql = "DELETE FROM Suppliers WHERE supplierID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, supplierID);
            ps.executeUpdate();
        }
        return "Supplier info deleted successfully.";
    }

    // Discount operations
    // Add a new discount which applies to a given product at a given store.
    public static String enterDiscountInfo(int discountID, int productID, int storeID, double discountDetails, String validStartDate, String validEndDate) throws SQLException {
        String sql = "INSERT INTO Discounts (discountID, productID, storeID, discountStartDate, discountEndDate, promotion) " +
                 "VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, discountID);
            ps.setInt(2, productID);
            ps.setInt(3, storeID);
            ps.setDate(4, java.sql.Date.valueOf(validStartDate));
            ps.setDate(5, java.sql.Date.valueOf(validEndDate));
            ps.setDouble(6, discountDetails);
            ps.executeUpdate();
        }
        return "Discount info entered successfully.";
    }

    // Update values associated to an existing Discount
    public static String updateDiscountInfo(int discountID, Integer productID, Integer storeID, Double discountDetails, String validStartDate, String validEndDate) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE Discounts SET ");
        boolean first = true;
        if (productID != null) { sql.append("productID = ?"); first = false; }
        if (storeID != null) { sql.append(first ? "" : ", ").append("storeID = ?"); first = false; }
        if (validStartDate != null) { sql.append(first ? "" : ", ").append("discountStartDate = ?"); first = false; }
        if (validEndDate != null) { sql.append(first ? "" : ", ").append("discountEndDate = ?"); first = false; }
        if (discountDetails != null) { sql.append(first ? "" : ", ").append("promotion = ?"); }
        sql.append(" WHERE discountID = ?");
    
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (productID != null) ps.setInt(index++, productID);
            if (storeID != null) ps.setInt(index++, storeID);
            if (validStartDate != null) ps.setDate(index++, java.sql.Date.valueOf(validStartDate));
            if (validEndDate != null) ps.setDate(index++, java.sql.Date.valueOf(validEndDate));
            if (discountDetails != null) ps.setDouble(index++, discountDetails);
            ps.setInt(index, discountID);
            ps.executeUpdate();
        }
        return "Discount info updated successfully.";
    }

    // Delete a existing discount
    public static String deleteDiscountInfo(int discountID) throws SQLException {
        String sql = "DELETE FROM Discounts WHERE discountID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, discountID);
            ps.executeUpdate();
        }
        return "Discount info deleted successfully.";
    }
    
    //Sign Up Methods
    //create a new club member sign up
    public static String enterSignUp(int storeID, int custID, String date, int staffID) throws SQLException {
    	String sql = "INSERT INTO SignUps (storeID, customerID, signUpDate, staffID) " +
                "VALUES (?, ?, ?, ?)";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, storeID);
            ps.setInt(2, custID);
            ps.setDate(3, java.sql.Date.valueOf(date));
            ps.setInt(4, staffID);
            ps.executeUpdate();
        }
        return "SignUp info entered successfully.";
    }
    
    //create a new club member sign up
    public static String updateSignUp(Integer storeID, Integer custID, String date, Integer staffID) throws SQLException {
    	StringBuilder sql = new StringBuilder("UPDATE SignUps SET ");
        boolean first = true;
        if (storeID != null) { sql.append("storeID = ?"); first = false; }
        if (custID != null) { sql.append(first ? "" : ", ").append("customerID = ?"); first = false; }
        if (date != null) { sql.append(first ? "" : ", ").append("signUpDate = ?"); first = false; }
        if (staffID != null) { sql.append(first ? "" : ", ").append("staffID = ?"); first = false; }
        sql.append(" WHERE customerID = ?");
    
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (storeID != null) ps.setInt(index++, storeID);
            if (custID != null) ps.setInt(index++, custID);
            if (date != null) ps.setDate(index++, java.sql.Date.valueOf(date));
            if (staffID != null) ps.setInt(index++, staffID);
            ps.setInt(index, custID);
            ps.executeUpdate();
        }
        return "SignUp info update successfully.";
    }
    
    // Delete a existing Sign Up Record
    public static String deleteSignUp(int custID) throws SQLException {
        String sql = "DELETE FROM SignUps WHERE customerID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, custID);
            ps.executeUpdate();
        }
        return "Sign Ups info deleted successfully.";
    }

    // ***********************************************************************
    // MANAGE INVENTORY RECORDS
    // These functions handle operations related to merchandise stock,
    // including inserting new inventory, updating stock levels, and removing
    // out-of-stock items.
    // ***********************************************************************

    // Inventory operations
    private static String insertInventory(int storeID, int productID, String productName, int stockQuantity, Double buyPrice, Double marketPrice, String productionDate, String expirationDate, int supplierID) throws SQLException {
        String sql = "INSERT INTO Merchandise (storeID, productID, productName, stockQuantity, buyPrice, marketPrice, productionDate, expirationDate, supplierID) " +
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, storeID);
            ps.setInt(2, productID);
            ps.setString(3, productName);
            ps.setInt(4, stockQuantity);
            ps.setDouble(5, buyPrice);
            ps.setDouble(6, marketPrice);
            ps.setDate(7, java.sql.Date.valueOf(productionDate));
            ps.setDate(8, java.sql.Date.valueOf(expirationDate));
            ps.setInt(9, supplierID);
            ps.executeUpdate();
        }
        return "Inventory info entered successfully.";
    }
    //Update Inventory
    private static String updateInventory(int storeID, int productID, String productName, Integer stockQuantity, Double buyPrice, Double marketPrice, String productionDate, String expirationDate, Integer supplierID) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE Merchandise SET ");
        boolean first = true;
        if (productName != null) { sql.append("productName = ?"); first = false; }
        if (stockQuantity != null) { sql.append(first ? "" : ", ").append("stockQuantity = ?"); first = false; }
        if (buyPrice != null) { sql.append(first ? "" : ", ").append("buyPrice = ?"); first = false; }
        if (marketPrice != null) { sql.append(first ? "" : ", ").append("marketPrice = ?"); first = false; }
        if (productionDate != null) { sql.append(first ? "" : ", ").append("productionDate = ?"); first = false; }
        if (expirationDate != null) { sql.append(first ? "" : ", ").append("expirationDate = ?"); first = false; }
        if (supplierID != null) { sql.append(first ? "" : ", ").append("supplierID = ?"); }
        sql.append(" WHERE storeID = ? AND productID = ?");
    
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (productName != null) ps.setString(index++, productName);
            if (stockQuantity != null) ps.setInt(index++, stockQuantity);
            if (buyPrice != null) ps.setDouble(index++, buyPrice);
            if (marketPrice != null) ps.setDouble(index++, marketPrice);
            if (productionDate != null) ps.setDate(index++, java.sql.Date.valueOf(productionDate));
            if (expirationDate != null) ps.setDate(index++, java.sql.Date.valueOf(expirationDate));
            if (supplierID != null) ps.setInt(index++, supplierID);
            ps.setInt(index++, storeID);
            ps.setInt(index, productID);
            ps.executeUpdate();
        }
        return "Inventory info updated successfully.";
    }

    //delete an inventory item from a certain store
    private static String deleteInventory(Integer storeID, Integer productID) throws SQLException {
        String sql = "DELETE FROM Merchandise WHERE storeID = ? AND productID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, storeID);
            ps.setInt(2, productID);
            ps.executeUpdate();
        }
        return "Discount info deleted successfully.";
    }

     //Transfer operations (for moving stock between stores)
    private static String processTransfer(Integer store1ID, Integer store2ID, Integer product1ID, Integer product2ID, String transferDate, Integer staffID) throws SQLException {

        try {
            //Start the transaction.
            connection.setAutoCommit(false);
            
            int changes = 0;
            String sql = "UPDATE Merchandise SET storeID = ?, productID = ? WHERE storeID = ? AND productID = ?";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setInt(1, store2ID);
            ps.setInt(2, product2ID);
            ps.setInt(3, store1ID);
            ps.setInt(4, product1ID);
            changes = ps.executeUpdate();
            
            //If there were no changes after executing the update, rollback
            if (changes == 0) {
            	connection.rollback();
            	connection.setAutoCommit(true);
            	return "Transfer failed.";
            }
            
            String sql2 = "INSERT INTO Transfers (store1ID, store2ID, product1ID, product2ID, transferDate, staffID) "  +
                    "VALUES (?, ?, ?, ?, ?, ?)";
            ps = connection.prepareStatement(sql2);
            ps.setInt(1, store1ID);
            ps.setInt(2, store2ID);
            ps.setInt(3, product1ID);
            ps.setInt(4, product2ID);
            ps.setDate(5, java.sql.Date.valueOf(transferDate));
            ps.setInt(6, staffID);
            changes = ps.executeUpdate();
            
          //If there were no changes after executing the insert, rollback
            if (changes == 0) {
            	connection.rollback();
            	connection.setAutoCommit(true);
            	return "Transfer failed.";
            //If there were changes, commit
            } else {
            	connection.commit();
            	connection.setAutoCommit(true);
            	return "Transfer processed successfully.";
            }
            
        } catch (Exception error) {
        	//If an error is thrown, rollback
        	if (connection != null) {
        		connection.rollback();
        		connection.setAutoCommit(true);
        		return "Transfer failed.";
        	//If there is no connection, output an error message
        	} else {
        		return "Connection null.";
        	}
        	
        } 
        
        
        
        
    }

//    private static String processTransfer(Integer store1ID, Integer store2ID, Integer productID, String transferDate, Integer staffID) throws SQLException {
//        if (store1ID == null || store2ID == null || productID == null || transferDate == null || staffID == null) {
//            throw new IllegalArgumentException("Missing required parameters.");
//        }
//
//        connection.setAutoCommit(false);
////        connection.setAutoCommit(false);
//        try {
//            // Insert first
//            String sql2 = "INSERT INTO Transfers (store1ID, store2ID, product1ID, transferDate, staffID) VALUES (?, ?, ?, ?, ?)";
//            try (PreparedStatement ps = connection.prepareStatement(sql2)) {
//                ps.setInt(1, store1ID);
//                ps.setInt(2, store2ID);
//                ps.setInt(3, productID);
//                ps.setDate(4, java.sql.Date.valueOf(transferDate));
//                ps.setInt(5, staffID);
//                ps.executeUpdate();
//            }
//
//            // Then update the Merchandise
//            String sql = "UPDATE Merchandise SET storeID = ? WHERE storeID = ? AND productID = ?";
//            try (PreparedStatement ps = connection.prepareStatement(sql)) {
//                ps.setInt(1, store2ID);
//                ps.setInt(2, store1ID);
//                ps.setInt(3, productID);
//
//                int updated = ps.executeUpdate();
//                if (updated == 0) {
//                    connection.rollback();
//                    return "No matching merchandise found to transfer.";
//                }
//            }
//
//            connection.commit();
//            return "Transfer processed successfully.";
//        } catch (SQLException e) {
//            connection.rollback();
//            throw e;
//        } finally {
//            connection.setAutoCommit(true);
//        }
//    }
    
    // ***********************************************************************
    // MAINTAIN BILLING AND TRANSACTION RECORDS
    // These functions handle financial transactions including generating
    // bills, updating bill amounts, managing customer rewards, and processing
    // purchase transactions.
    // ***********************************************************************

    // Billing operations
    //Create a bill to be paid to a supplier or not
    private static String generateBill(Integer billID, Double amountOwed, String status, Integer staffID, Integer supplierID) throws SQLException {
    	String sql = "INSERT INTO Bills (billID, amountOwed, status, staffID, supplierID) " +
                "VALUES (?, ?, ?, ?, ?)";
       try (PreparedStatement ps = connection.prepareStatement(sql)) {
           ps.setInt(1, billID);
           ps.setDouble(2, amountOwed);
           ps.setString(3, status);
           ps.setInt(4, staffID);
           ps.setInt(5, supplierID);
           ps.executeUpdate();
       }
       return "Billing info entered successfully.";
    }

    //update an existing Bill amount
    public static String updateBill(Integer billID, Double amountOwed, String status, Integer staffID, Integer supplierID) throws SQLException {
    	StringBuilder sql = new StringBuilder("UPDATE Bills SET ");
        boolean first = true;
        if (billID != null) { sql.append("billID = ?"); first = false; }
        if (amountOwed != null) { sql.append(first ? "" : ", ").append("amountOwed = ?"); first = false; }
        if (status != null) { sql.append(first ? "" : ", ").append("status = ?"); first = false; }
        if (staffID != null) { sql.append(first ? "" : ", ").append("staffID = ?"); first = false; }
        if (supplierID != null) { sql.append(first ? "" : ", ").append("supplierID = ?"); first = false; }
    
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            if (billID != null) ps.setInt(index++, billID);
            if (amountOwed != null) ps.setDouble(index++, amountOwed);
            if (status != null) ps.setString(index++, status);
            if (staffID != null) ps.setInt(index++, staffID);
            if (supplierID != null) ps.setInt(index++, supplierID);
            ps.executeUpdate();
        }
        return "Billing info updated successfully.";
    }
    
    // Delete a existing Bill
    public static String deleteBill(int billID) throws SQLException {
        String sql = "DELETE FROM Bills WHERE billID = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, billID);
            ps.executeUpdate();
        }
        return "Bills info deleted successfully.";
    }


    // Reward operations
    //Create a reward object if the input customer is a Platinum Member with their membership active
    private static String createReward(Integer rewardID, Double checkAmountOwed, Integer staffID, Integer customerID, String startDate, String endDate) throws SQLException {
        String insertSQL = "INSERT INTO Rewards (rewardID, checkAmountOwed, staffID, customerID) VALUES (?, ?, ?, ?)";
        String selectSQL = "SELECT membershipLevel, custStatus FROM ClubMembers WHERE customerID = ?";

        checkAmountOwed = 0.0;

        try {
        	//Start the transaction.
            connection.setAutoCommit(false);

            int changes = 0;
            try (PreparedStatement ps = connection.prepareStatement(insertSQL)) {
                ps.setInt(1, rewardID);
                ps.setDouble(2, checkAmountOwed);
                ps.setInt(3, staffID);
                ps.setInt(4, customerID);
                changes = ps.executeUpdate();
            }
	    //If there are no changes from the insert statement, rollback
            if (changes == 0) {
                connection.rollback();
                return "Reward creation failed.";
            }

            try (PreparedStatement ps2 = connection.prepareStatement(selectSQL)) {
                ps2.setInt(1, customerID);
                try (ResultSet rs = ps2.executeQuery()) {
                    if (rs.next()) {
                        String level = rs.getString("membershipLevel");
                        String status = rs.getString("custStatus");

                        if ("Platinum".equals(level) && "Active".equals(status)) {
                            String message = calculateReward(customerID, startDate, endDate);
                            //If the reward calculation and update fails, rollback
                            if (!"Reward calculated successfully.".equals(message)) {
                                connection.rollback();
                                return "Reward calculation failed.";
                            }
                            //If the reward is calculated and updated successfully, commit.
                            connection.commit();
                            return "Reward successfully created.";
                        //If the customer is not an active platinum customer, rollback
                        } else {
                            connection.rollback();
                            return "Not an active platinum customer.";
                        }
                    //If there is no customer with the given ID in the database, rollback
                    } else {
                        connection.rollback();
                        return "Invalid club member ID.";
                    }
                }
            }
        //Handle an exception
        } catch (Exception error) {
        	//Rollback if the connection is valid.
        	if (connection != null) {
        		connection.rollback();
        		return "Reward calculation failed.";
        		// Return an error if the connection is null.
        	} else {
        		return "Connection null.";
        	}
        // If the connection is not null, reset autocommit to true before returning.
        } finally {
        	if (connection != null) {
            	connection.setAutoCommit(true);
        	}
        }
    }
//    private static String createReward(Integer rewardID, Double checkAmountOwed, Integer staffID, Integer customerID, String startDate, String endDate) throws SQLException {
//    	String sql = "INSERT INTO Rewards (rewardID, checkAmountOwed, staffID, customerID) " +
//                "VALUES (?, ?, ?, ?)";
//    	checkAmountOwed = 0.0;
//    	try {
//    		connection.setAutoCommit(false);
//	    	int changes = 0;
//	    	PreparedStatement ps = connection.prepareStatement(sql);
//	        ps.setInt(1, rewardID);
//	        ps.setDouble(2, checkAmountOwed);
//	        ps.setInt(3, staffID);
//	        ps.setInt(4, customerID);
//	        changes = ps.executeUpdate();
//	    	
//	    	//If there are no changes from the insert statement, rollback
//	    	if (changes == 0) {
//	    		connection.rollback();
//	       		connection.setAutoCommit(true);
//	       		return "Reward creation failed.";
//	    	}
//	    	
//		   	String sql2 = "SELECT membershipLevel, custStatus FROM ClubMembers WHERE customerID =  ?;";
//		   	PreparedStatement ps2 = connection.prepareStatement(sql2);
//		    ps2.setInt(1, customerID);
//	        ResultSet rs = ps2.executeQuery();
//	        if (rs.next()) {
//	        	String level = rs.getString("membershipLevel");
//	        	String status = rs.getString("custStatus");
//	            if(level.equals("Platinum") && status.equals("Active")) {
//	            	String message = calculateReward(customerID, startDate, endDate);
//	            	//If the reward is not calculated and updated successfully, rollback
//	            	if (!message.equals("Reward calculated successfully.")) {
//	            		connection.rollback();
//	            		connection.setAutoCommit(true);
//	            		return "Reward calculation failed.";
//	            	//If the reward is calculated and updated successfully, commit
//	            	} else {
//	            		connection.commit();
//	            		connection.setAutoCommit(true);
//	            		return "Reward successfully created.";
//	            	}
//	            //If the club member is not platinum or not active, rollback
//	            } else {
//	            	connection.rollback();
//            		connection.setAutoCommit(true);
//            		return "Not an active platinum customer.";
//	            }
//	        //If there is no club member with the given ID, rollback
//	        } else {
//	        	connection.rollback();
//        		connection.setAutoCommit(true);
//	            return "Invalid club member ID.";
//	        }
//	   
//    	} catch (SQLException error) {
//    		//If there is an error, rollback
//        	if (connection != null) {
//        		connection.rollback();
//        		connection.setAutoCommit(true);
//        		return "Reward creation failed.";
//        	//If the connection is null, report that error
//        	} else {
//        		return "Connection null.";
//        	}
//    	}
//
//    }
    
    // Calculate reward based on customerID and transaction history
    private static String calculateReward(int customerID, String startDate, String endDate) throws SQLException {
    	String sql = "UPDATE Rewards SET checkAmountOwed=(SELECT COALESCE(SUM(totalPrice) * .02, 0) FROM Transactions WHERE purchaseDate>=? AND " +
    			" purchaseDate<? AND customerID = ?) WHERE customerID = ?";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setDate(1, java.sql.Date.valueOf(startDate));
            ps.setDate(2, java.sql.Date.valueOf(endDate));
            ps.setInt(3, customerID);
            ps.setInt(4, customerID);
            if (ps.executeUpdate() == 0) {
            	return "Reward calculation failed.";
            } else {
            	return "Reward calculated successfully.";
            }
        } catch (SQLException error) {
        	return "Reward calculation failed.";
        }
        
    }
    
//    private static String updateReward(int checkID, String startDate, String endDate) throws SQLException{
//    	String sql = "SELECT customerID FROM Rewards WHERE rewardID =  ?;";
//	   	
//    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
//    		ps.setInt(1, checkID);
//    		ResultSet rs = ps.executeQuery();
//    		if (rs.next()) {
//    			int customerID = rs.getInt("customerID");
//    			String message = calculateReward(customerID, startDate, endDate);
//    			if (!message.equals("Reward calculated successfully.")) {
//            		return "Successful update.";
//    			}
//        	
//    		} 
//    		return "Update failed.";
//    	} catch (SQLException error) {
//    		return "Update failed.";
//    	}
//    }
    
    // Update reward amount based on changes in customer purchases or their membership status
    private static String updateReward(Integer rewardID, Double checkAmountOwed, Integer staffID, Integer customerID, String startDate, String endDate) throws SQLException {
        String sql = "UPDATE Rewards SET checkAmountOwed = ?, staffID = ?, customerID = ? WHERE rewardID = ?;";
        try {
//            connection.setAutoCommit(false);

            int changes = 0;
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setDouble(1, checkAmountOwed);
                ps.setInt(2, staffID);
                ps.setInt(3, customerID);
                ps.setInt(4, rewardID);
                changes = ps.executeUpdate();
            }

            if (changes == 0) {
//                connection.rollback();
                return "Reward update failed.";
            }

            String sql2 = "SELECT membershipLevel, custStatus FROM ClubMembers WHERE customerID = ?;";
            try (PreparedStatement ps = connection.prepareStatement(sql2)) {
                ps.setInt(1, customerID);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String level = rs.getString("membershipLevel");
                        String status = rs.getString("custStatus");

                        if (level.equals("Platinum") && status.equals("Active")) {
                            String message = calculateReward(customerID, startDate, endDate);
                            if (!message.equals("Reward calculated successfully.")) {
//                                connection.rollback();
                                return "Reward calculation failed.";
                            } else {
//                                connection.commit();
                                return "Reward successfully updated.";
                            }
                        } else {
//                            connection.rollback();
                            return "Not an active platinum customer.";
                        }
                    } else {
//                        connection.rollback();
                        return "Invalid club member ID.";
                    }
                }
            }
        } catch (SQLException error) {
            if (connection != null) {
//                connection.rollback();
            }
            return "Reward update failed.";
        } 
//        finally {
//            connection.setAutoCommit(true);
//        }
    }
    
    // Transaction operations
    //Inserts a transaction with given attributes
    private static String insertTransaction(int transactionID, String purchaseDate, Double totalPrice, int customerID, int staffID, int storeID, String productList) throws SQLException {
    	String sql = "INSERT INTO Transactions (transactionID, purchaseDate, totalPrice, customerID, staffID, storeID, productList) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
       try (PreparedStatement ps = connection.prepareStatement(sql)) {
           ps.setInt(1, transactionID);
           ps.setDate(2, java.sql.Date.valueOf(purchaseDate));
           ps.setDouble(3, totalPrice);
           ps.setInt(4, customerID);
           ps.setInt(5, staffID);
           ps.setInt(6, storeID);
           ps.setString(7, productList);
           ps.executeUpdate();
       }
       return "Transaction entered successfully.";
    }
    
    //calculates and enters new Transaction based on purchases of user and taking into account updating merchandise stock
    private static String calculateTransaction(int transactionID, String purchaseDate, int customerID, int staffID, int storeID, String productList, String amounts) throws SQLException {
        String[] items = productList.split(",");
        String[] parts = amounts.split(",");
        
        if (items.length != parts.length) {
            throw new IllegalArgumentException("Mismatch between number of products and amounts.");
        }

        int[] trueAmounts = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            trueAmounts[i] = Integer.parseInt(parts[i].trim());
        }

        double trueTotal = 0.0;
        int amountCounter = 0;

        for (String item : items) {
            item = item.trim();
            int amount = trueAmounts[amountCounter];

            System.out.println("Processing item: " + item + " | Amount: " + amount);
            
            int changes = 0;
            
//            connection.setAutoCommit(false);

            // Step 1: Get product info from Merchandise
            String merchSQL = "SELECT productID, marketPrice, stockQuantity FROM Merchandise WHERE LOWER(productName) = LOWER(?) AND storeID = ?;";
            try (PreparedStatement ps1 = connection.prepareStatement(merchSQL)) {
                ps1.setString(1, item);
                ps1.setInt(2, storeID);

                try (ResultSet rs1 = ps1.executeQuery()) {
                    if (rs1.next()) {
                        int productID = rs1.getInt("productID");
                        double price = rs1.getDouble("marketPrice");
                        int stock = rs1.getInt("stockQuantity");

                        // Step 2: Check if discount exists
                        double discount = 0.0;
                        String discountSQL = "SELECT promotion FROM Discounts WHERE productID = ? AND storeID = ? AND discountStartDate <= ? AND discountEndDate >= ?;";
                        try (PreparedStatement ps2 = connection.prepareStatement(discountSQL)) {
                            ps2.setInt(1, productID);
                            ps2.setInt(2, storeID);
                            ps2.setDate(3, java.sql.Date.valueOf(purchaseDate));
                            ps2.setDate(4, java.sql.Date.valueOf(purchaseDate));

                            try (ResultSet rs2 = ps2.executeQuery()) {
                                if (rs2.next()) {
                                    discount = rs2.getDouble("promotion");
                                }
                            }
                        }

                        // Step 3: Calculate final price
                        double discountedPrice = price * ((100.0 - discount)*0.01);
                        double totalForItem = discountedPrice * amount;
                        trueTotal += totalForItem;

                        // Step 4: Update stock
                        int newStock = stock - amount;
                        String updateStockSQL = "UPDATE Merchandise SET stockQuantity = ? WHERE productID = ? AND storeID = ?;";
                        try (PreparedStatement ps3 = connection.prepareStatement(updateStockSQL)) {
                            ps3.setInt(1, newStock);
                            ps3.setInt(2, productID);
                            ps3.setInt(3, storeID);
                            ps3.executeUpdate();
                        }

                        amountCounter++;

                    } else {
                        System.out.println("No product found for: " + item);
                    }
                }
            }
        }

        // Step 5: Record transaction
        insertTransaction(transactionID, purchaseDate, trueTotal, customerID, staffID, storeID, productList);

        return "Transaction recorded successfully.";
    }
    
//    private static String calculateTransaction(int transactionID, String purchaseDate, int customerID, int staffID, int storeID, String productList, String amounts) throws SQLException 
//    {
//    	String[] items = productList.split(",");
//    	String[] parts = amounts.split(",");
//
//    	// Convert String[] to int[]
//    	int[] trueAmounts = new int[parts.length];
//    	for (int i = 0; i < parts.length; i++) {
//    	    trueAmounts[i] = Integer.parseInt(parts[i].trim());
//    	}
//    	Double trueTotal = 0.0;
//    	int amountCounter = 0;
//    	for (String item : items) {
//    		item = item.trim();
//    		System.out.println("Searching for item: '" + item + "'");
//    		 System.out.println("Processing item: " + item);
//    		 System.out.println("Amount: " + trueAmounts[amountCounter]);
////    	    System.out.println(item.trim());
//    		String sql = "SELECT m.productID, m.marketPrice, d.promotion, m.stockQuantity " +
//    	             "FROM Merchandise AS m " +
//    	             "LEFT JOIN Discounts AS d ON m.productID = d.productID AND m.storeID = d.storeID " +
//    	             "AND d.discountStartDate <= ? AND d.discountEndDate >= ? " +
//    	             "WHERE m.productName = ? AND m.storeID = ?;";
//    		   try (PreparedStatement ps = connection.prepareStatement(sql)) {
//    		       ps.setString(1, item);
//    		       ps.setInt(2, storeID);
//    		       ps.setDate(3, java.sql.Date.valueOf(purchaseDate));
//    		       ps.setDate(4, java.sql.Date.valueOf(purchaseDate));
//    	           try (ResultSet rs = ps.executeQuery()) {
//    	        	   System.out.println("");
//    	               if (rs.next()) {
//    	                   Double price = rs.getDouble("marketPrice");
//    	                   Double disc = rs.getObject("promotion") != null ? rs.getDouble("promotion") : 0.0;
//    	                   int quant = rs.getInt("stockQuantity");
//    	                   int id = rs.getInt("productID");
//    	                   Double tempPrice = price * (1 - disc);
//    	                   tempPrice *= trueAmounts[amountCounter];
//    	                   trueTotal += tempPrice;
//    	                   int newAmount = quant - trueAmounts[amountCounter];
//    	                   amountCounter++;
//    	                   String sql2 = "UPDATE Merchandise SET stockQuantity = ? WHERE productID = ? AND storeID = ?;";
//    	                   try (PreparedStatement ps2 = connection.prepareStatement(sql2)) {
//    	        		       ps2.setInt(1, newAmount);
//    	        		       ps2.setInt(2, id);
//    	        		       ps2.setInt(3, storeID);
//    	        		       ps2.executeUpdate();
//    	                   }
//	    	           }
//	               } 
//               }
//		   }
//    	//enter in transaction record
//    	insertTransaction(transactionID, purchaseDate, trueTotal, customerID, staffID, storeID, productList);
//
//	   
//       return "Reward info entered successfully.";
//    	}
    	

    private static void updateTransactionTotal() throws SQLException {
        // TODO: Implement total calculation logic
    }

    // ***********************************************************************
    // REPORT GENERATION
    // These functions generate reports to analyze business performance,
    // including sales, stock levels, customer activity, and financial summaries.
    // ***********************************************************************

    private static void generateReports() throws SQLException {
        // TODO: Implement report generation logic
    }
  //Returns the total amount of sales for the store chain on the input purchaseDate
    private static String calculateSalesByDay(String purchaseDate) throws SQLException {
    	String sql = "SELECT SUM(totalPrice) FROM Transactions WHERE purchaseDate = ?;";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setDate(1, java.sql.Date.valueOf(purchaseDate));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    double totalSales = rs.getDouble("SUM(totalPrice)");
                    return "Sales on " + purchaseDate + ": $" + totalSales;
                    } 
                else {
                    return "No sales on " + purchaseDate;
                    }
                }
            }
    	}
    
    //Returns the total amount of sales for the store chain between the two input dates.
    private static String calculateSalesByMonth(String startDate, String endDate) throws SQLException {
    	String sql = "SELECT SUM(totalPrice) FROM Transactions WHERE purchaseDate >= ? AND purchaseDate< ?;";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setDate(1, java.sql.Date.valueOf(startDate));
            ps.setDate(2, java.sql.Date.valueOf(endDate));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    double totalSales = rs.getDouble("SUM(totalPrice)");
                    return "Total Sales between " + startDate + " and " + endDate + ": $" + totalSales;
                    } 
                else {
                    return "No sales made in input month.";
                    }
                }
            }
    	}
    
    //Returns the total amount of sales for the store chain between the two input dates.
    private static String calculateSalesByYear(String startDate, String endDate) throws SQLException {
    	String sql = "SELECT SUM(totalPrice) FROM Transactions WHERE purchaseDate >= ? AND purchaseDate< ?;";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
    		ps.setDate(1, java.sql.Date.valueOf(startDate));
            ps.setDate(2, java.sql.Date.valueOf(endDate));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String totalSales = Double.toString(rs.getDouble("SUM(totalPrice)"));
                    return "Total Sales between " + startDate + " and " + endDate + ": $" + totalSales;
                    } 
                else {
                    return "No sales made in input year";
                    }
                }
            }
    	}
    
    //Calculates the total sales made on each day between the startDate and the endDate and returns a string with the total cumulative sales growth
    private static String calculateSalesGrowth(Integer storeID, String startDate, String endDate) throws SQLException {
    	String sql = "SELECT t.purchaseDate, SUM(t.totalPrice) as total, SUM(SUM(t.totalPrice)) OVER () AS cumulative_total FROM Transactions t, StaffMembers s " +
    "WHERE t.storeID = ? AND s.staffID=t.staffID AND t.purchaseDate>= ? AND t.purchaseDate< ? " +
    			"GROUP BY t.purchaseDate ORDER BY t.purchaseDate;";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
    		ps.setInt(1, storeID);
    		ps.setDate(2, java.sql.Date.valueOf(startDate));
            ps.setDate(3, java.sql.Date.valueOf(endDate));
    		try(ResultSet rs = ps.executeQuery()){
    			boolean hasResults = false;
    			String ans = "";
    			String cumulative_total = "";
    			while (rs.next()) {
    			    hasResults = true;
    			    String totalSales = Double.toString(rs.getDouble("total"));
    			    ans = ans + "Total Sales on " + rs.getString("purchaseDate") + ": $" + totalSales + "\n";
    			    cumulative_total = Double.toString(rs.getDouble("cumulative_total"));
    			}
    			if (!hasResults) {
    			    return "No sales growth reported for store over the interval";
    			}
    			ans = ans + "Total Overall Sales Between " + startDate + " and " + endDate + ": $" + cumulative_total + "\n";
    			return ans;
    		}
    	}
    }
    
    //Get the stock of all merchandise in a store
    private static String getMerchStockByStore(Integer storeID) throws SQLException {
    	String sql = "SELECT productName, SUM(stockQuantity) as quant FROM Merchandise " +
    "WHERE storeID = ? GROUP BY productName;";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, storeID);
            try (ResultSet rs = ps.executeQuery()) {
            	String ans = "";
            	boolean hasResults = false;
                while (rs.next()) {
                    String totalItems = Integer.toString(rs.getInt("quant"));
                    ans = ans + rs.getString("productName") + " Stock at store " + Integer.toString(storeID) + ": " + totalItems + "\n";
                    hasResults = true;
                    } 
                if (!hasResults) {
                    return "Store not found";
                    }
                return ans;
                }
            }
    }
    
    //Get the total stock of an item for all stores in the chain
    private static String getMerchStockByItem(String name) throws SQLException {
    	String sql = "SELECT SUM(stockQuantity) as quant FROM Merchandise " +
    "WHERE productName = ?;";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String totalItems = Integer.toString(rs.getInt("quant"));
                    return name + " Stock at All stores: " + totalItems;
                    } 
                else {
                    return "Merch not found";
                    }
                }
            }
    }
    
    //Get the total number of customers added between two input dates
    private static String getCustGrowthReport(String startDate, String endDate) throws SQLException {
    	String sql = "SELECT COUNT(*) as quant FROM ClubMembers c JOIN SignUps s ON c.customerID = s.customerID WHERE s.signUpDate >= ? AND s.signUpDate < ?;";
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setDate(1, java.sql.Date.valueOf(startDate));
            ps.setDate(2, java.sql.Date.valueOf(endDate));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String totalItems = Integer.toString(rs.getInt("quant"));
                    return "Number of Customers Added: " + totalItems;
                    } 
                else {
                    return "No Customers Added";
                    }
                }
            }
    }
    
    //Get the total amount of money a customer spent between two input dates.
    private static String getCustActivityReport(Integer custID, String startDate, String endDate) throws SQLException{
    	String sql = "SELECT customerID, SUM(totalPrice) AS TotalPurchases FROM Transactions WHERE " +
    			"purchaseDate>= ? AND purchaseDate < ? AND customerID = ? GROUP BY customerID ORDER BY customerID;";
    	try (PreparedStatement ps = connection.prepareStatement(sql)) {
    		ps.setDate(1, java.sql.Date.valueOf(startDate));
            ps.setDate(2, java.sql.Date.valueOf(endDate));
            ps.setInt(3, custID);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String totalItems = Double.toString(rs.getDouble("TotalPurchases"));
                    return Integer.toString(custID) + " Total Purchases: " + totalItems;
                    } 
                else {
                    return "No Purchases Found";
                    }
                }
            }
    }

}

package com.example.finalass;/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <YOUR STUDENT ID HERE>
 *
 */

import com.example.finalass.Credentials;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.control.TableView;
import javafx.scene.layout.GridPane;

import java.io.*;
//import java.io.OutputStreamWriter;

import java.net.Socket;

import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;

import static com.example.finalass.BooksDatabaseClient.thePrimaryStage;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
    //these clasess are not exported by the module. Instead, one needs to impor
    //javax.sql.rowset.* as above.



public class BooksDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for author's name and one for library's name.
    private ResultSet outcome   = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public BooksDatabaseService(Socket aSocket){
        
		//TO BE COMPLETED
        serviceSocket = aSocket;
        this.start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For author
        this.requestStr[1] = ""; //For library
		
		String tmp = "";
        try {

			//TO BE COMPLETED
            InputStream socketstream = this.serviceSocket.getInputStream();
            InputStreamReader socketReader = new InputStreamReader(socketstream);
            StringBuffer stringBuffer = new StringBuffer();
            char x;
            while (true) //Read until terminator character is found
            {
                x = (char) socketReader.read();
                if (x == '#')
                    break;
                stringBuffer.append(x);
            }
            String[] spol  = stringBuffer.toString().split(";");

            this.requestStr[0] = spol[0];
            this.requestStr[1] = spol[1];

        }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		
		this.outcome = null;
		
		String sql = "select book.title,book.publisher,book.genre,book.rrp,count(book.title) as copies from author inner join book on author.authorid = book.authorid inner join bookcopy on bookcopy.bookid " +
                "= book.bookid inner join library on library.libraryid = bookcopy.libraryid where author.familyname = ? and library.city = ? group by book.publisher, book.title, book.genre, book.rrp;"; //TO BE COMPLETED- Update this line as needed.
		
		
		try {
			//Connet to the database
			//TO BE COMPLETED
            Class.forName("org.postgresql.Driver");
            Connection con = DriverManager.getConnection(URL,USERNAME,PASSWORD);

            PreparedStatement pstmt = con.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            pstmt.clearParameters();
            pstmt.setString(1,this.requestStr[0]);
            pstmt.setString(2,this.requestStr[1]);
            ResultSet rs = pstmt.executeQuery();
            //Process query
            //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();
            crs.populate(rs);
            outcome = crs;

            rs.beforeFirst();
            while (rs.next()){
                System.out.println(rs.getString("title") + " | " + rs.getString("publisher") + " | " + rs.getString("genre") + " | " + rs.getString("rrp") + " | " + rs.getString("copies")+"\n");
            }

			//Clean up
			//TO BE COMPLETED
            rs.close();
            con.close();

		} catch (Exception e)
		{ System.out.println(e); }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
			//Return outcome
			//TO BE COMPLETED
            ObjectOutputStream OutcomeStream = new ObjectOutputStream(serviceSocket.getOutputStream());
            OutcomeStream.writeObject(outcome);
            System.out.println("Service thread" + this.threadId() + ": Service outcome returned; " + this.outcome);

            
			//Terminating connection of the service socket
			//TO BE COMPLETED
            serviceSocket.close();
			
        }catch (IOException e){
            System.out.println("Service thread " + this.threadId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "author->" + this.requestStr[0] + "; library->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}

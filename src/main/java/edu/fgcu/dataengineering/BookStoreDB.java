package edu.fgcu.dataengineering;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
// just to check if these are found

public class BookStoreDB {

    public static void insertAuthor() {

        String path = "src/Data/BookStore.db";
        Connection conn = null;

        try {

            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + path);
            System.out.println(" Connected to Database! ");

            Gson gson = new Gson();
            JsonReader jread = new JsonReader(new FileReader("src/Data/authors.json"));
            AuthorParser[] authors = gson.fromJson(jread, AuthorParser[].class);
            System.out.println("Inserting values");

            for (var element : authors) {
                System.out.println(element.getName() + " " + element.getEmail() + " " + element.getUrl());

                String name = element.getName();
                String email = element.getEmail();
                String url = element.getUrl();

                if (url.equals("")) {
                    int x = 0;
                    url = "url does not exist " + name + x;
                    x++;
                }

                String sql = "INSERT INTO author(author_name,author_email,author_url) VALUES(?,?,?)";

                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, name);
                pstmt.setString(2, email);
                pstmt.setString(3, url);
                pstmt.executeUpdate();
            }

            System.out.println("Values in table");

        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();

        } catch (SQLException ex) {
            ex.printStackTrace();

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } finally {

            try {

                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertBook() {

        String path = "src/Data/BookStore.db";
        Connection conn = null;

        try {

            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + path);
            System.out.println(" Connected to Database! ");
            String book = "src/Data/bookstore_report2.csv";

            CSVReader temp = new CSVReader(new FileReader(book));

            String[] SEOExample;
            System.out.println("Inserting books");
            temp.readNext();

            while ((SEOExample = temp.readNext()) != null) {

                String isbn = SEOExample[0];
                String title = SEOExample[1];
                String author = SEOExample[2];
                String publisher = SEOExample[3];
                String location = SEOExample[4];

                String sql = "INSERT INTO book(isbn,publisher_name,author_name, book_title) VALUES(?,?,?,?)";

                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, isbn);
                pstmt.setString(2, publisher);
                pstmt.setString(3, author);
                pstmt.setString(4, title);
                pstmt.executeUpdate();

            }

            System.out.println("Inserted books");

        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();

        } catch (SQLException ex) {
            ex.printStackTrace();

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (CsvValidationException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();

        } finally {

            try {

                if (conn != null) {
                    conn.close(); //close the connection
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}



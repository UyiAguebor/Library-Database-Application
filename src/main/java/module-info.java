module com.example.finalass {
    requires javafx.controls;
    requires javafx.fxml;
    requires java.sql.rowset;


    opens com.example.finalass to javafx.fxml;
    exports com.example.finalass;
}
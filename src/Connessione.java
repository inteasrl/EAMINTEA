import java.io.Console;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Logger;

import com.google.gson.Gson;

public class Connessione {

    Server server;
    Connection connessione;
    String url;

    Logger logger = GlobalLogger.getLogger();

    private Gson converter = new Gson();
    private Console console = System.console();

    //Crea connessionene per Salvataggio
    public Connessione (ArrayList <Server> connessioni) { 
        server = new Server(console);
        url = "jdbc:sqlserver://" + server.host + ";instanceName="+ server.istanza + ";databaseName=" + server.db +";trustServerCertificate=true";
        try {
            this.connessione = DriverManager.getConnection(url, server.user, server.password); 
            System.out.println("Connessione riuscita al server");
            
            save(connessioni, console, converter);
        } catch (Exception e) {
            
            System.out.println(e);
        }
    }

    public Connessione (Server server) { 
        
        url = "jdbc:sqlserver://" + server.host + ";instanceName="+ server.istanza + ";databaseName=" + server.db +";trustServerCertificate=true";
        try {
            this.connessione = DriverManager.getConnection(url, server.user, server.password); 
            System.out.println("Connessione riuscita al server " + server.host + "\\" + server.istanza);
            logger.info("Connessione riuscita al server " + server.host + "\\" + server.istanza);
        } catch (Exception e) {
            logger.severe(e.toString());
            System.out.println(e);
        }
    }
    
    //Salva la connessione sul json delle connessioni per il riutilizzo 
    public void save (ArrayList <Server> connessioni, Console console, Gson converter) {
        while(true){
            String response = console.readLine("Si desidera salvare la connessione? [y/n]");
            if(response.equals("y") && connessioni.size() < 2){
                connessioni.add(server);
                String path = "./res/connectios.json";
                String json = converter.toJson(connessioni);
                try (FileWriter writer = new FileWriter(path)) {
                    writer.write(json);
                    System.out.println("Salvato con successo in " + path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            } else if (connessioni.size() >= 2){
                System.out.println("Lista connessioni piena, resettare per inserire nuove connessioni");
                break;
            }   
        }   
    }

    //Salva la connessione sul json delle connessioni per il riutilizzo 
    public Connection getConnection () {
        return this.connessione;
    }
}

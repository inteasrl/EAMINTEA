import java.io.Console;
import java.io.FileWriter;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.logging.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

public class Main {
    public static void main(String[] args)  {
    

        ArrayList <Server> connessioni = new ArrayList<>();
        Gson parser = new Gson();
        int scelta;
        Logger logger = GlobalLogger.getLogger();

        connessioni = updateListFromJson(connessioni, parser);

      
        
        while(true){
            clearOutput();
            System.out.println("*** EAM INTEA DATABASE SYNC UTILITY ***");
            System.out.println("1] Nuova connessione            2] Elenca connessioni \n3] Cancella Connessioni         4] Copia Tabelle\n5] Aggiorna Tabelle            ");
            try {
                scelta = Integer.parseInt(System.console().readLine(""));
                switch (scelta) {
                case 1:
                    new Connessione(connessioni);
                    break;

                case 2:
                    clearOutput();
                    int i = 0;
                    for(Server server : connessioni){
                        System.out.println("indice: "+ i);;
                        System.out.println(server.toString());
                        System.out.println("");
                        i++;
                    }
                    System.out.println();
                    System.console().readLine("Return per continuare ");
                    clearOutput();
                    break;
                case 3:
                    deletConnectionsRecord(connessioni);
                    break;
                case 5:
                    update(connessioni);;
                    break;
                case 4:
                    clone(connessioni);
                    break;
            }
            } catch (Exception e) {
                return;
            }
        }
    }

    static void clearOutput () {
        for (int i = 0; i < 50; i++) 
        System.out.println(); 
    }

    static ArrayList<Server> updateListFromJson (ArrayList<Server> listaConnessioni, Gson parser) {
        try {
            String content = Files.readString(Paths.get("./res/connectios.json"));
            Type listType = new TypeToken<ArrayList<Server>>(){}.getType();
            listaConnessioni = parser.fromJson(content, listType); 
        } catch (IOException e) {
            e.printStackTrace();
           
        }
        return listaConnessioni;
    }

    static void deletConnectionsRecord(ArrayList<Server> listaconnessioni) {
        Console console = System.console();
        String response = console.readLine("Si desidera eliminare il record delle connessioni? [y/n]");
        
        while(true){
            if(response.equals("y")){
                
                listaconnessioni.clear();
                
                String path = "./res/connectios.json";
                try (FileWriter writer = new FileWriter(path)) {
                    writer.write("[]");
                    System.out.println("eliminati con successo con successo in " + path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                console.readLine("Elementi elimintai con successo any key to continue");
                break;
            } else  {
                break;
            }    
        }
        clearOutput();
    }

    static void clone (ArrayList <Server> connessioni) {
        clearOutput();
        Logger logger = GlobalLogger.getLogger();
        CloneAgent agent = new CloneAgent(connessioni);
        try {
            agent.cloneCondotte("ACQ_CONDOTTA", "GIS2EAM_OGGETTI");
            agent.cloneContatori("EAM2GIS_OGGETTI", "CONTATORI");
        } catch (Exception e) {
            logger.severe(e.toString());
            System.out.println(e);
        }

        System.out.println();
        System.console().readLine("Return per continuare ");
    }

    static void update (ArrayList <Server> connessioni) {
        clearOutput();
        Logger logger = GlobalLogger.getLogger();
        CloneAgent agent = new CloneAgent(connessioni);
        try {
            agent.updateContatori("EAM2GIS_OGGETTI", "CONTATORI");
            agent.updateCondotte("ACQ_CONDOTTA", "GIS2EAM_OGGETTI");
        } catch (Exception e) {
            logger.severe(e.toString());
            System.out.println(e);
        }

        System.out.println();
        System.console().readLine("Return per continuare ");
    }
}
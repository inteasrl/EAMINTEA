

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.sql.Timestamp;

import java.util.logging.*;

public class CloneAgent {

    public Connessione intea;
    public Connessione acoset;

    Logger logger = GlobalLogger.getLogger();

    public CloneAgent (ArrayList <Server> connessioni){
        intea = new Connessione(connessioni.get(0));
        acoset = new Connessione(connessioni.get(1));
    }

    //si tengono astratti i nomi tabella di Source e Target in quanto potrebbero cambiare
    public void updateCondotte (String tabellaSource, String tabellaTarget) throws Exception{
        System.out.println("\nInizio Aggiornamento Condotte");
        logger.fine("Inizio Aggiornamento Condotte");
        Timestamp updateTime = Timestamp.valueOf(LocalDate.now().atStartOfDay());
        
        PreparedStatement updated = intea.getConnection().prepareStatement("SELECT * FROM  " + tabellaSource + " WHERE DATA_AGG >= ?");
        updated.setTimestamp(1, updateTime);
        ResultSet copy = updated.executeQuery();

        PreparedStatement deletedItems = acoset.getConnection().prepareStatement("SELECT * FROM " + tabellaTarget);
        ResultSet ceck = deletedItems.executeQuery();

        //controlla che tutti i record presenti nella tabella di target esistano in quella di source, se no, sono stati cancellati da quella di source e vengono cancellati anche da quella di target
        int count = 0;
        int cancellati = 0;
        while(ceck.next()){
            int ex = 0;
            PreparedStatement exist = intea.getConnection().prepareStatement("SELECT * FROM  " + tabellaSource + " WHERE OBJECTID = ?");
            exist.setString(1, decrement(ceck.getString("G2E_IDGIS")));
            ResultSet existRes = exist.executeQuery();
            
            //Controlla il nuomero di volte che e' stato trovato questo io, se 0 allora e' staot cancellato
            while(existRes.next()){
                ex++;
            }
           
            if(ex == 0){
                PreparedStatement delete = acoset.getConnection().prepareStatement("DELETE FROM " + tabellaTarget + " WHERE G2E_IDGIS = ?");
                delete.setString(1, decrement(ceck.getString("G2E_IDGIS")));
                delete.executeUpdate();
                cancellati ++;
                System.out.println("elemento cancellato con OBJECTID " + ceck.getString("G2E_IDGIS"));
                logger.info("elemento cancellato con OBJECTID " + ceck.getString("G2E_IDGIS"));
            }
            count++;
            System.out.print("\rcontrollo eliminazioni: " + count);
        }
        System.out.println();


        //cancella dalla tabella di target i record che sono stati aggiornati in quella di source
        int v = 0;
        while(copy.next()){
            System.out.println("Aggiornato elemento con OBJECTID " + copy.getString("OBJECTID"));
            logger.info("Aggiornato elemento con OBJECTID " + copy.getString("OBJECTID"));
            try{
                PreparedStatement delete = acoset.getConnection().prepareStatement("DELETE FROM " + tabellaTarget + " WHERE G2E_IDGIS = ?");
                delete.setString(1, increment(copy.getString("OBJECTID")));
                delete.executeUpdate();
           } catch (Exception e) {
                System.out.println(e);
           }
           v++;
        }

        if(v == 0){
            System.out.println("Non ci sono state modifiche recenti alla tabella " + tabellaSource + " tutto up to date su " + tabellaTarget);
            logger.info("Non ci sono state modifiche recenti alla tabella " + tabellaSource + " tutto up to date su " + tabellaTarget);
        }

        
        PreparedStatement ps = acoset.getConnection().prepareStatement("INSERT INTO " + tabellaTarget + " (G2E_IDGIS, G2E_CLASS, G2E_COMUNE, G2E_INDIRIZZO, G2E_STATUS, G2E_ATTR1_DESC, G2E_ATTR1_VALUE, G2E_ATTR2_DESC, G2E_ATTR2_VALUE, G2E_ATTR3_DESC, G2E_ATTR3_VALUE, G2E_DATA_CREAZIONE, G2E_DATA_MODIFICA, G2E_TYPE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        
        copy = updated.executeQuery();
        while(copy.next()){
            ps.setInt(1, copy.getInt("OBJECTID") + 5000000);
            ps.setString(2,  "CONDOTTA");
            ps.setString(3, copy.getString("ISTAT"));
            ps.setString(4, copy.getString("VIA_DENOMINAZIONE"));
            ps.setString(5, copy.getString("D_STATO"));
            ps.setString(6, "DISTRETTO");
            ps.setString(7, copy.getString("DISTRETTO"));
            ps.setString(8, "MATERIALE");
            ps.setString(9, copy.getString("MATERIALE"));
            ps.setString(10, "DIAMETRO");
            ps.setString(11, copy.getString("CLASSE_DN"));
            ps.setTimestamp(12, Timestamp.valueOf(copy.getString("DATA_INS")));
            ps.setTimestamp(13, Timestamp.valueOf(copy.getString("DATA_AGG")));
            ps.setInt(14, copy.getInt("TYPE"));
            ps.executeUpdate(); 
        }
    }

    public void updateContatori (String tabellaSource, String tabellaTarget) throws Exception{
        System.out.println("\nInizio Aggiornamento Contatori");
        logger.fine("Inizio Aggiornamento Contatori");

        Timestamp updateTime = Timestamp.valueOf(LocalDate.now().atStartOfDay());

        PreparedStatement updated = acoset.getConnection().prepareStatement("SELECT * FROM  " + tabellaSource + " WHERE E2G_DATA_MODIFICA >= ?");
        updated.setTimestamp(1, updateTime);
        ResultSet copy = updated.executeQuery();

        PreparedStatement deletedItems = intea.getConnection().prepareStatement("SELECT * FROM " + tabellaTarget);
        ResultSet ceck = deletedItems.executeQuery();

        //controlla che tutti i record presenti nella tabella di target esistano in quella di source, se no, sono stati cancellati da quella di source e vengono cancellati anche da quella di target
        int count = 0;
        int cancellati = 0;
        while(ceck.next()){
            int ex = 0;
            PreparedStatement exist = acoset.getConnection().prepareStatement("SELECT * FROM  " + tabellaSource + " WHERE E2G_CODE = ?");
            exist.setString(1, ceck.getString("PUF_CODE"));
            ResultSet existRes = exist.executeQuery();
            
            while(existRes.next()){
                ex++;
            }
           
            if(ex == 0){
                PreparedStatement delete = intea.getConnection().prepareStatement("DELETE FROM " + tabellaTarget + " WHERE PUF_CODE = ?");
                delete.setString(1, ceck.getString("PUF_CODE"));
                delete.executeUpdate();
                cancellati ++;
                System.out.println("elemento cancellato con OBJECTID " + ceck.getString("PUF_CODE"));
                logger.info("elemento cancellato con OBJECTID " + ceck.getString("PUF_CODE"));
            }
            count++;
            System.out.print("\rcontrollo eliminazioni: " + count);
        }
        System.out.println();


        //cancella dalla tabella di target i record che sono stati aggiornati in quella di source
        int v = 0;
        while(copy.next()){
            System.out.println("elemento aggiornato con CODICE_EAM " + copy.getString("E2G_CODE"));
            logger.info("elemento aggiornato con CODICE_EAM " + copy.getString("E2G_CODE"));
            try{
                PreparedStatement delete = intea.getConnection().prepareStatement("DELETE FROM " + tabellaTarget + " WHERE PUF_CODE = ?");
                delete.setString(1, copy.getString("E2G_CODE"));
                delete.executeUpdate();
           } catch (Exception e) {
                System.out.println(e);
           }
           v++;
        }

        if(v == 0){
            System.out.println("Non ci sono state modifiche recenti alla tabella " + tabellaSource + " tutto up to date su " + tabellaTarget);
            logger.info("Non ci sono state modifiche recenti alla tabella " + tabellaSource + " tutto up to date su " + tabellaTarget);
        }

        
        PreparedStatement ps = intea.getConnection().prepareStatement("INSERT INTO " + tabellaTarget + " (OBJECTID, COD_PRESA, COMUNE, VIA_DENOMINAZIONE, DATA_INS, DATA_AGG, D_STATO, POINT_X, POINT_Y, PUF_CODE) VALUES (?,?,?,?,?,?,?,?,?,?)");
        
        //trova il piu grande Object Id nella tabella di target e accoda i nuovi record o i record modificati in quella di source agli elementi della tabella di target
        PreparedStatement idlist = intea.getConnection().prepareStatement("SELECT * FROM  " + tabellaTarget);
        copy = idlist.executeQuery();
        int objid = getHigherID(copy) + 1;

        copy = updated.executeQuery();
        while(copy.next()){
            ps.setInt(1, objid);
            ps.setString(2, copy.getString("E2G_ATTR2_VALUE"));
            ps.setString(3, copy.getString("E2G_COMUNE"));
            ps.setString(4, copy.getString("E2G_INDIRIZZO"));
            ps.setString(5, copy.getString("E2G_DATA_CREAZIONE"));
            ps.setString(6, copy.getString("E2G_DATA_MODIFICA"));
            ps.setString(7, copy.getString("E2G_STATUS"));
            ps.setString(8, copy.getString("E2G_COOX"));
            ps.setString(9, copy.getString("E2G_COOY"));
            ps.setString(10, copy.getString("E2G_CODE"));
            ps.executeUpdate(); 
            objid++;
        }
    }

    public void cloneCondotte (String tabellaSource, String tabellaTarget) throws Exception {
        System.out.println("\nInizio copia Condotte");
        PreparedStatement all = intea.getConnection().prepareStatement("SELECT * FROM " + tabellaSource);
        ResultSet copy = all.executeQuery();
      
        PreparedStatement delete = acoset.getConnection().prepareStatement("DELETE FROM " + tabellaTarget);
        delete.executeUpdate();

        PreparedStatement ps = acoset.getConnection().prepareStatement("INSERT INTO " + tabellaTarget + " (G2E_IDGIS, G2E_CLASS, G2E_COMUNE, G2E_INDIRIZZO, G2E_STATUS, G2E_ATTR1_DESC, G2E_ATTR1_VALUE, G2E_ATTR2_DESC, G2E_ATTR2_VALUE, G2E_ATTR3_DESC, G2E_ATTR3_VALUE, G2E_DATA_CREAZIONE, G2E_DATA_MODIFICA, G2E_TYPE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        int count = 0;
      
        while(copy.next()){
            ps.setInt(1, copy.getInt("OBJECTID") + 5000000);
            ps.setString(2,  "CONDOTTA");
            ps.setString(3, copy.getString("COMUNE"));
            ps.setString(4, copy.getString("VIA_DENOMINAZIONE"));
            ps.setString(5, copy.getString("D_STATO"));
            ps.setString(6, "DISTRETTO");
            ps.setString(7, copy.getString("DISTRETTO"));
            ps.setString(8, "MATERIALE");
            ps.setString(9, copy.getString("MATERIALE"));
            ps.setString(10, "DIAMETRO");
            ps.setString(11, copy.getString("CLASSE_DN"));
            ps.setTimestamp(12, Timestamp.valueOf(copy.getString("DATA_INS")));
            ps.setTimestamp(13, Timestamp.valueOf(copy.getString("DATA_AGG")));
            ps.setInt(14, copy.getInt("TYPE"));
            ps.executeUpdate(); 
            count++;
            System.out.print("\rScrivo condotte: " + count);
        }

        System.out.println("\nCopiati " + count + " elementi in " + tabellaTarget);
        logger.info("Copiati " + count + " elementi in " + tabellaTarget);
    }

    static int getHigherID (ResultSet rs) {
        int higher = 1;
        try{
            while(rs.next()){
            if(rs.getInt("OBJECTID") >= higher){
                higher = rs.getInt("OBJECTID");
            }
            }
        } catch (Exception e){
            System.out.println(e);
        }
        return higher;
    }

    public void cloneContatori (String tabellaSource, String tabellaTarget) throws Exception{
        System.out.println("\nInizio copia Contatori");
        logger.fine("Inizio copia Contatori");
       
        PreparedStatement all = acoset.getConnection().prepareStatement("SELECT * FROM " + tabellaSource);
        ResultSet copy = all.executeQuery();

        PreparedStatement delete = intea.getConnection().prepareStatement ("DELETE FROM " + tabellaTarget);
        delete.executeUpdate();

        PreparedStatement ps = intea.getConnection().prepareStatement("INSERT INTO " + tabellaTarget + " (OBJECTID, COD_PRESA, COMUNE, VIA_DENOMINAZIONE, DATA_INS, DATA_AGG, D_STATO, POINT_X, POINT_Y, PUF_CODE) VALUES (?,?,?,?,?,?,?,?,?,?)");
        
        int objid = 0;
        while(copy.next()){
            ps.setInt(1, objid);
            ps.setString(2, copy.getString("E2G_ATTR2_VALUE"));
            ps.setString(3, copy.getString("E2G_COMUNE"));
            ps.setString(4, copy.getString("E2G_INDIRIZZO"));
            ps.setString(5, copy.getString("E2G_DATA_CREAZIONE"));
            ps.setString(6, copy.getString("E2G_DATA_MODIFICA"));
            ps.setString(7, copy.getString("E2G_STATUS"));
            ps.setString(8, copy.getString("E2G_COOX"));
            ps.setString(9, copy.getString("E2G_COOY"));
            ps.setString(10, copy.getString("E2G_CODE"));
            ps.executeUpdate(); 
            objid++;
            System.out.print("\rScrivo condotte: " + objid);
        }

        System.out.println("\nCopiati " + objid + " elementi in " + tabellaTarget);
        logger.info("Copiati " + objid + " elementi in " + tabellaTarget);
    }

    private String decrement (String init) throws Exception{
        int trasposed = Integer.parseInt(init) - 5000000;
        return String.valueOf((trasposed));
    }

    private String increment (String init) throws Exception{
        int trasposed = Integer.parseInt(init) + 5000000;
        return String.valueOf((trasposed));
    }
    
}



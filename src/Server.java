
import java.io.Console;

public class Server {

    public String nome;
    public String host;
    public String istanza;
    public String db;
    public String user;
    public String password;

    
    public Server (Console console) {
        this.nome = console.readLine("nome del server: ");
        this.host = console.readLine("Host del server: ");
        this.istanza = console.readLine("Nome istanza: ");
        this.db = console.readLine("Nome database: ");
        this.user = console.readLine("Username: ");
        this.password = console.readLine("password: ");
    }

    @Override
    public String toString () {
        return "Nome: " + this.nome + "\nHost: " + this.host + "\nIstanza: " + this.istanza + "\nDatabase: " + this.db;
    }
}

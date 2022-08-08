# my-orm

Biblioteca de mapeamento objeto-relacional implementada com a API de reflection do Java.
Utilizada apenas para fim acadêmicos. NÂO utilizar em aplicações reais em produção.

## Instalação

Faça o install do projeto no seu repositório local do maven. Logo depois, importe o artefato no arquivo pom.xml do seu projeto:
```xml
  <dependencies>
      <dependency>
          <groupId>com.dimer</groupId>
          <artifactId>my-orm</artifactId>
          <version>1.0-SNAPSHOT</version>
      </dependency>
  </dependencies>
```

Para configurar a conexão com o banco de dados, basta implementar o método `createConnection` da classe abstrata `ConnectionFactory`:

```java
public class CustomConnectionFactory extends ConnectionFactory {

    private static final String URL = "jdbc:postgresql://exemplo/database";

    @Override
    public Connection createConnection() {
        try {
            Properties props = new Properties();
            props.setProperty("user", "usuario");
            props.setProperty("password", "senha");
            return DriverManager.getConnection(URL, props);
        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Não conseguiu conectar com o banco");
        }
    }
}
```

E no startup da aplicação, passar uma instância da classe que implementa sua conexão através do método estático `setFactory`:

```java
ConnectionFactory.setFactory(new CustomConnectionFactory());
```

Para utilizar, as entidades que representam uma tabela no banco de dados devem implementar a interface Entity do pacote da biblioteca.

### Exemplo de uso
```java
Repository<Pessoa, Integer> repository = Repository.of(Pessoa.class);

Pessoa pessoa = repository.find(5);
pessoa.setNome("Nome");
repository.save(pessoa);
```

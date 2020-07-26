import com.google.common.collect.Iterators;
import org.neo4j.driver.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main implements AutoCloseable {

	private final Driver driver;

	public Main(String uri, String user, String password) {
		driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
	}

	public static void main(String... args) {
		try (Main main = new Main("bolt://localhost:7687", "neo4j", "neo4j")) {
			main.doWork();
		}
	}

	private void doWork() {
		try (Session session = driver.session()) {
			Result result = session.run("MATCH (n:JustNode) RETURN id(n) as id");
			Iterators.partition(result.stream().iterator(), 10000).forEachRemaining(records -> process(records, session));
		}
	}

	private void process(List<Record> records, Session session) {
		System.out.println("Processing batch...");
		session.writeTransaction(tx -> {
			List<Long> ids = records.stream().map(r -> r.get("id").asLong()).collect(Collectors.toList());
			return tx.run("MATCH (n) where id(n) in $ids set n:NewLabel", Map.of("ids", ids));
		});
	}

	@Override
	public void close() {
		driver.close();
	}

}

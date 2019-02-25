package name.liwuest.util.config;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import name.liwuest.util.db.CDBConnection;
import name.liwuest.util.db.CNamedPreparedStatement;
import name.liwuest.util.db.CNamedPreparedStatement.COpenedNamedPreparedStatement;
import name.liwuest.util.db.EDatabase;

public class CConfiguration {
	/** <p>Static initializer to setup data structure. */
	static {
		try (Connection conn = CDBConnection.connect(true); Statement stmt = conn.createStatement()) {
			stmt.execute("CREATE TABLE IF NOT EXISTS _configuration (domain VARCHAR(255) NOT NULL, key VARCHAR(255) NOT NULL, version BIGINT NOT NULL, created TIMESTAMP NOT NULL DEFAULT now(), value JSON, PRIMARY KEY (domain, key, version))");
		} catch (SQLException | EDatabase e) { throw new ExceptionInInitializerError(e); }
	}
	
	/** <p>Registered configurators preconfigured for a specific domain. */
	private static Map<String, CConfiguration> m_RegisteredConfigurations = new TreeMap<>();
	/** <p>Jackson Databind JSON-Object-Mapper to use. Domains may register their own module for serialization and deserialization.</p> */
	private static ObjectMapper m_JSONMapper = new ObjectMapper();
	/** <p>Thread pool used to decouple serialization of Java Objects to JSON and database writes.</p> */
	private static ExecutorService m_Executor = Executors.newCachedThreadPool();
	private final static CNamedPreparedStatement m_ReadConfigValue = new CNamedPreparedStatement("SELECT domain, key, version, created, value FROM _configuration WHERE domain = :domain AND key = :key ORDER BY version DESC LIMIT :limitCount");
	private final static CNamedPreparedStatement m_InsertConfigValue = new CNamedPreparedStatement("INSERT INTO _configuration (domain, key, version, value) SELECT :domain, :key, coalesce(max(version)+1, 1), :value FROM _configuration WHERE domain = :domain AND key = :key");
	private final String m_Domain;
	private CConfiguration(String Domain) { m_Domain = Domain; }
	public final static synchronized CConfiguration getConfig(String Domain) {
		if (!m_RegisteredConfigurations.containsKey(Domain)) { m_RegisteredConfigurations.put(Domain, new CConfiguration(Domain)); }
		return m_RegisteredConfigurations.get(Domain);
	}
	
	
	public final synchronized <T> LinkedList<T> get(String Key, T Default) throws SQLException, EDatabase, JsonParseException, JsonMappingException, IOException { return get(m_Domain, Key, Default, 1); }
	public final static synchronized <T> LinkedList<T> get(String Domain, String Key, T Default) throws SQLException, EDatabase, JsonParseException, JsonMappingException, IOException { return get(Domain, Key, Default, 1); }
	public final synchronized <T> LinkedList<T> get(String Key, T Default, long Count) throws SQLException, EDatabase, JsonParseException, JsonMappingException, IOException { return get(m_Domain, Key, Default, Count); }
	@SuppressWarnings("unchecked") public final static synchronized <T> LinkedList<T> get(String Domain, String Key, T Default, long Count) throws SQLException, EDatabase, JsonParseException, JsonMappingException, IOException {
		LinkedList<T> result = (LinkedList<T>)getByClass(Domain, Key, (Class<T>)(Default.getClass()), Count);
		if (result.isEmpty()) { result.add(Default); }
		return result;
	}
	public final synchronized <T> LinkedList<T> getByClass(String Key, Class<T> Type) throws JsonParseException, JsonMappingException, SQLException, EDatabase, IOException { return getByClass(m_Domain, Key, Type, 1); }
	public final static synchronized <T> LinkedList<T> getByClass(String Domain, String Key, Class<T> Type) throws JsonParseException, JsonMappingException, SQLException, EDatabase, IOException { return getByClass(Domain, Key, Type, 1); }
	public final synchronized <T> LinkedList<T> getByClass(String Key, Class<T> Type, long Count) throws SQLException, EDatabase, JsonParseException, JsonMappingException, IOException { return getByClass(m_Domain, Key, Type, Count); }
	public final static synchronized <T> LinkedList<T> getByClass(String Domain, String Key, Class<T> Type, long Count) throws SQLException, EDatabase, JsonParseException, JsonMappingException, IOException {
		LinkedList<T> result = new LinkedList<>();
		try (Connection conn = CDBConnection.connect(); COpenedNamedPreparedStatement stmt = m_ReadConfigValue.open(conn)) {
			stmt.setString("domain",  Domain);
			stmt.setString("key", Key);
			stmt.setLong("limitCount", Count);
			try (ResultSet rSet = stmt.executeQuery()) {
				while (rSet.next()) {
					result.addLast(m_JSONMapper.readValue(rSet.getString("value"), Type));
				}
			}
		}
		return result;
	}
	
	
	public final synchronized <T> T set(String Key, T Value) throws SQLException, EDatabase, JsonGenerationException, JsonMappingException, IOException, InterruptedException {
		// (1) use piping to reduce memory load; limit pipe to 16kbyte
		PipedInputStream pis = new PipedInputStream(16384);
		// (2) use multi-threading to avoid blocking of memory
		CountDownLatch latch = new CountDownLatch(1);
		m_Executor.submit(() -> {
			try { m_JSONMapper.writeValue(new PipedOutputStream(pis), Value); } finally { latch.countDown(); }
			return true;
		});
		// (3) This would be "simple" alternative if all the above threading and memory handling is not required
//		m_JSONMapper.writeValue(new PipedOutputStream(pis), Value);
		try (Connection conn = CDBConnection.connect(); COpenedNamedPreparedStatement stmt = m_InsertConfigValue.open(conn)) {
			stmt.setString("domain", m_Domain);
			stmt.setString("key", Key);
			stmt.setBinaryStream("value", pis);
			stmt.execute();
		}
		latch.await();
		return Value;
	}
}

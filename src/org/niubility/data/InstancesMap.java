package org.niubility.data;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

public class InstancesMap implements Map<String, SparseVector>, Closeable {
        private Environment env;
        private Database db;
        private StoredClassCatalog catalog;
        private String dir = "./";
        private Map<String, SparseVector> map;

        public InstancesMap() {

                // environment is transactional
                EnvironmentConfig envConfig = new EnvironmentConfig();
                envConfig.setAllowCreate(true);
                try {
					env = new Environment(new File(dir), envConfig);
				} catch (EnvironmentLockedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                // use a generic database configuration
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setDeferredWrite(true);
                dbConfig.setAllowCreate(true);

                // catalog is needed for serial bindings (java serialization)
                Database catalogDb;
				try {
					catalogDb = env.openDatabase(null, "catalog", dbConfig);
					catalog = new StoredClassCatalog(catalogDb);
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

                try {
					this.db = env.openDatabase(null, "db", dbConfig);
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                // use Integer tuple binding for key entries
                TupleBinding<String> keyBinding = TupleBinding
                                .getPrimitiveBinding(String.class);

                // use String serial binding for data entries
                SerialBinding<SparseVector> dataBinding = new SerialBinding<SparseVector>(
                                catalog, SparseVector.class);
                // create a map view of the database
                this.map = new StoredSortedMap<String, SparseVector>(db, keyBinding,
                                dataBinding, true);
        }

        @Override
        public void clear() {
                map.clear();
        }

        @Override
        public boolean containsKey(Object key) {
                return map.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
                return map.containsKey(value);
        }

        @Override
        public Set<java.util.Map.Entry<String, SparseVector>> entrySet() {
                return map.entrySet();
        }

        @Override
        public SparseVector get(Object key) {
                return map.get(key);
        }

        @Override
        public boolean isEmpty() {
                return map.isEmpty();
        }

        @Override
        public Set<String> keySet() {
                return map.keySet();
        }

        @Override
        public SparseVector put(String key, SparseVector value) {
                return map.put(key, value);
        }

        @Override
        public void putAll(Map<? extends String, ? extends SparseVector> m) {
                map.putAll(m);
        }

        @Override
        public SparseVector remove(Object key) {
                return map.remove(key);
        }

        @Override
        public int size() {
                return map.size();
        }

        @Override
        public Collection<SparseVector> values() {
                return map.values();
        }

        @Override
        public void close() throws IOException {
                map.clear();
                try {
					db.close();
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                try {
					catalog.close();
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                try {
					env.close();
				} catch (DatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        }

        public static void main(String args[]) throws IOException {
                InstancesMap m = new InstancesMap();
                SparseVector v = new SparseVector();
                v.put("dafds", 10.0f);
                m.put("dsfasdf", v);
                System.out.println(m.get("dsfasdf"));
                m.close();
        }
}
package ma.s2m.fraudmanager.service;

import ma.s2m.fraudmanager.service.db.RocksDBService;
import ma.s2m.fraudmanager.service.db.StorageConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class RocksDBServiceTest {

    @TempDir
    Path tempDir;

    private RocksDBService rocksDBService;
    private StorageConfig storageConfig;

    @BeforeEach
    void setUp() {
        storageConfig = new StorageConfig(
                1,
                new ArrayList<>(List.of(0)),
                "test-node",
                1000L,
                100L,
                1,
                null
        );
    }

    @AfterEach
    void tearDown() {
        if (rocksDBService != null) {
            rocksDBService.close();
        }
    }

    @Test
    void testShardDirectoryCreation() {
        String dbPath = tempDir.toString();
        
        // Initialize service - this should create the directory
        assertDoesNotThrow(() -> {
            rocksDBService = new RocksDBService(dbPath, 100, storageConfig);
        });

        // Verify directory exists
        File shardDir = new File(dbPath, "shard-0");
        assertTrue(shardDir.exists(), "Shard directory should be created");
        assertTrue(shardDir.isDirectory(), "Shard path should be a directory");
    }

    @Test
    void testCrossThreadBatchFlushing() throws Exception {
        String dbPath = tempDir.toString();
        rocksDBService = new RocksDBService(dbPath, 100, storageConfig);

        String key = "card:12345:1000";
        ma.s2m.fraudmanager.model.Measurment m = new ma.s2m.fraudmanager.model.Measurment();
        m.setKey(key);
        m.setWindowSize(1000L);

        // 1. Simulate worker thread writing to batch
        Thread worker = new Thread(() -> {
            rocksDBService.setMeasurmentByKey(key, m);
        });
        worker.start();
        worker.join();

        // 2. Main thread flushes the batch
        // With ThreadLocal, this would fail to see the worker's data.
        // With ConcurrentHashMap, this should work.
        rocksDBService.flushBatch();

        // 3. Wait a bit for async writer to persist to RocksDB
        Thread.sleep(200);

        // 4. Verify data exists in RocksDB
        ma.s2m.fraudmanager.model.Measurment retrieved = rocksDBService.getMeasurmentByKey(key);
        org.junit.jupiter.api.Assertions.assertNotNull(retrieved, "Measurment should be persisted and retrievable");
        org.junit.jupiter.api.Assertions.assertEquals(key, retrieved.getKey(), "Retrieved key should match");
    }
}

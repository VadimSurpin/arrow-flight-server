package net.surpin.data.arrowflight.server.adapters;

import net.surpin.data.arrowflight.server.model.AppConfig;
import net.surpin.data.arrowflight.server.model.FileAssignment;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParquetAdapterTest {

    /** Verifies construction populates cached table DDL metadata. */
    @Test
    void constructorInitializesCatalogDdlCache() throws Exception {
        Path dataDirectory = new Path("/data");
        Path schemaDirectory = new Path(dataDirectory, "test_schema");
        Path tableDirectory = new Path(schemaDirectory, "test_table");
        FileSystem fileSystem = new CatalogFileSystem(
                dataDirectory, schemaDirectory, tableDirectory);
        AppConfig config = new AppConfig(
                3, 4096, 4, 65536, 2, 2, 2, null, false,
                null, null, null, null, false, 67108864, 67108864, 30000L,
                dataDirectory.toString(), null, 31001, 5701, 120, 3, 500, 0);

        ParquetAdapter adapter = new CatalogTestParquetAdapter(config, fileSystem);

        String ddl = adapter.tableDdlCache()
                .getOrDefault("test_schema", java.util.Collections.emptyMap())
                .get("test_table");
        assertNotNull(ddl);
        assertTrue(ddl.startsWith("CREATE TABLE test_schema.test_table"));
    }

    /** Verifies full Arrow schemas and footer row counts are reused from metadata caches. */
    @Test
    void cachesArrowSchemaAndParquetFooterStatistics() throws IOException {
        Path dataDirectory = new Path("/data");
        Path schemaDirectory = new Path(dataDirectory, "test_schema");
        Path tableDirectory = new Path(schemaDirectory, "test_table");
        Path parquetPath = new Path(tableDirectory, "part.parquet");
        FileSystem fileSystem = mock(FileSystem.class);
        when(fileSystem.listStatus(dataDirectory))
                .thenReturn(new FileStatus[]{directory(schemaDirectory)});
        when(fileSystem.listStatus(schemaDirectory))
                .thenReturn(new FileStatus[]{directory(tableDirectory)});
        FileStatus rootStatus = mock(FileStatus.class);
        when(rootStatus.getPath()).thenReturn(dataDirectory);
        when(fileSystem.getFileStatus(dataDirectory)).thenReturn(rootStatus);
        LocatedFileStatus parquetStatus = mock(LocatedFileStatus.class);
        when(parquetStatus.isFile()).thenReturn(true);
        when(parquetStatus.getPath()).thenReturn(parquetPath);
        when(parquetStatus.getLen()).thenReturn(100L);
        when(parquetStatus.getModificationTime()).thenReturn(1L);
        RemoteIterator<LocatedFileStatus> iterator = remoteIterator(parquetStatus);
        when(fileSystem.listFiles(tableDirectory, true)).thenReturn(iterator);

        MessageType parquetSchema = Types.buildMessage()
                .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
                .named("id")
                .named("test_table");
        org.apache.parquet.hadoop.metadata.FileMetaData fileMetadata =
                mock(org.apache.parquet.hadoop.metadata.FileMetaData.class);
        when(fileMetadata.getSchema()).thenReturn(parquetSchema);
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(7L);
        ParquetMetadata footer = mock(ParquetMetadata.class);
        when(footer.getFileMetaData()).thenReturn(fileMetadata);
        when(footer.getBlocks()).thenReturn(List.of(block));
        ParquetFileReader reader = mock(ParquetFileReader.class);
        when(reader.getFooter()).thenReturn(footer);

        AppConfig config = new AppConfig(
                3, 4096, 4, 65536, 2, 2, 2, null, false,
                null, null, null, null, false, 67108864, 67108864, 30000L,
                dataDirectory.toString(), null, 31001, 5701, 120, 3, 500, 0);
        try (MockedStatic<ParquetFileReader> parquetReader =
                     mockStatic(ParquetFileReader.class)) {
            parquetReader.when(() -> ParquetFileReader.open(any()))
                    .thenReturn(reader);
            ParquetAdapter adapter = new ParquetAdapter(config, fileSystem);

            Schema first = adapter.getTableSchema("test_schema", "test_table");
            Schema second = adapter.getTableSchema("test_schema", "test_table");
            assertSame(first, second);
            Map<String, FileAssignment> assignment = Map.of(
                    "test_schema/test_table/part.parquet",
                    new FileAssignment(100L, Set.of("local")));

            long firstEstimate = adapter.estimateRowCount(assignment);
            long secondEstimate = adapter.estimateRowCount(assignment);

            assertEquals(7L, firstEstimate);
            assertEquals(firstEstimate, secondEstimate);
            verify(fileSystem, times(1)).listFiles(tableDirectory, true);
            parquetReader.verify(() -> ParquetFileReader.open(any()), times(1));
        }
    }

    /**
     * Creates a one-element Hadoop remote iterator.
     *
     * @param value iterator value
     * @return one-element iterator
     */
    private static RemoteIterator<LocatedFileStatus> remoteIterator(
            LocatedFileStatus value) {
        return new RemoteIterator<>() {
            private boolean available = true;

            @Override
            public boolean hasNext() {
                return available;
            }

            @Override
            public LocatedFileStatus next() {
                available = false;
                return value;
            }
        };
    }

    /** Creates a directory status for mocked filesystem listings. */
    private static FileStatus directory(Path path) {
        return new FileStatus(0L, true, 1, 128L, 0L, path);
    }

    /** Supplies a stable schema while the superclass initializes its catalog cache. */
    private static final class CatalogTestParquetAdapter extends ParquetAdapter {

        /** Creates an adapter backed by mocked directory metadata. */
        private CatalogTestParquetAdapter(AppConfig config, FileSystem fileSystem) {
            super(config, fileSystem);
        }

        @Override
        public Schema getTableSchema(String schema, String table) {
            Field id = new Field("id",
                    FieldType.nullable(new ArrowType.Int(32, true)), null);
            return new Schema(List.of(id));
        }
    }

    /** Supplies deterministic schema and table directory listings. */
    private static final class CatalogFileSystem extends FileSystem {

        private final Path root;
        private final Path schema;
        private final Path table;

        /** Creates a filesystem with one schema and one table. */
        private CatalogFileSystem(Path root, Path schema, Path table) {
            this.root = root;
            this.schema = schema;
            this.table = table;
        }

        @Override
        public URI getUri() {
            return URI.create("test:///");
        }

        @Override
        public FSDataInputStream open(Path path, int bufferSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream create(Path path, FsPermission permission,
                boolean overwrite, int bufferSize, short replication,
                long blockSize, Progressable progress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path source, Path destination) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path path) throws FileNotFoundException {
            if (root.equals(path)) {
                return new FileStatus[] {directory(schema)};
            }
            if (schema.equals(path)) {
                return new FileStatus[] {directory(table)};
            }
            throw new FileNotFoundException(path.toString());
        }

        @Override
        public void setWorkingDirectory(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory() {
            return root;
        }

        @Override
        public boolean mkdirs(Path path, FsPermission permission) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            if (root.equals(path) || schema.equals(path) || table.equals(path)) {
                return directory(path);
            }
            throw new FileNotFoundException(path.toString());
        }
    }

    @Test
    void hasLocalBlockMatchesResolvedFlightAndDataNodeAliases() {
        assertTrue(ParquetAdapter.hasLocalBlock(Set.of("127.0.0.1"), "localhost"));
        assertFalse(ParquetAdapter.hasLocalBlock(Set.of("127.0.0.2"), "localhost"));
    }

    @Test
    void validateNameValid() {
        assertEquals("test_table", ParquetAdapter.validateName("test_table"));
        assertEquals("a", ParquetAdapter.validateName("a"));
        assertEquals("_leading_underscore", ParquetAdapter.validateName("_leading_underscore"));
        assertEquals("camelCase_name123", ParquetAdapter.validateName("camelCase_name123"));
    }

    @Test
    void validateNameNull() {
        assertNull(ParquetAdapter.validateName(null));
    }

    @Test
    void validateNameStartsWithDigit() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("0abc"));
    }

    @Test
    void validateNameContainsDot() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("table.name"));
    }

    @Test
    void validateNamePathTraversal() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName(".."));
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("../etc"));
    }

    @Test
    void validateNameSqlInjection() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName("x'; DROP TABLE"));
    }

    @Test
    void validateNameEmpty() {
        assertThrows(IllegalArgumentException.class, () -> ParquetAdapter.validateName(""));
    }

    @Test
    void createLikePredicateMatchAll() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("%");
        assertTrue(p.test("anything"));
        assertTrue(p.test(""));
        assertTrue(p.test("very_long_table_name"));
    }

    @Test
    void createLikePredicateNullReturnsMatchAll() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate(null);
        assertTrue(p.test("abc"));
    }

    @Test
    void createLikePredicateExactMatch() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("test_table");
        assertTrue(p.test("test_table"));
        assertFalse(p.test("other_table"));
    }

    @Test
    void createLikePredicateWildcard() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("test_%");
        assertTrue(p.test("test_table"));
        assertTrue(p.test("test_other"));
        assertFalse(p.test("prod_table"));
    }

    @Test
    void createLikePredicateSingleCharWildcard() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("t_st");
        assertTrue(p.test("test"));
        assertTrue(p.test("tast"));
        assertFalse(p.test("toast"));
    }

    @Test
    void createLikePredicateCaseInsensitive() throws Exception {
        Predicate<String> p = invokeCreateLikePredicate("TEST");
        assertTrue(p.test("test"));
        assertTrue(p.test("TEST"));
    }

    private static Predicate<String> invokeCreateLikePredicate(String pattern) throws Exception {
        Method m = ParquetAdapter.class.getDeclaredMethod("createLikePredicate", String.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Predicate<String> result = (Predicate<String>) m.invoke(null, pattern);
        return result;
    }
}

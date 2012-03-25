## Summary
FileStoredMap is an implementation of java.util.Map. The Entry(key-value) is stored in files instead of Java heap avoiding the Out Of Memory Error. 
For example, below:

    @Test
    public void testPutPojo() throws IOException {
        TestUtils.deleteFiles("tmp/empdir");
        FileStoredMap<Employee> map = null;
        try {
            map = new FileStoredMap<Employee>("tmp/empdir");
            Date createdAt = new Date();
            Employee emp = TestUtils.createEmployee("hoge", 256, createdAt);
            map.put("emp", emp);
            TestUtils.assertEmployeeEquivalent(emp, map.get("emp"));
        } finally {
            if (map != null) {
                map.close();
            }
        }
    }

File format is BSON(http://bsonspec.org/).

## Maven Repository
- Jars: http://wrap-trap.net/maven2/snapshot/net/wrap-trap/collections/FileStoredMap/0.0.1-SNAPSHOT/
- Repository URL: http://wrap-trap.net/maven2/snapshot/

pom.xml:

    <dependencies>
        <dependency>
            <groupId>net.wrap-trap.collections</groupId>
            <artifactId>FileStoredMap</artifactId>
            <version>0.0.1-SNAPSHOT</version>
        </dependency>
        ...
    </dependencies>
    ...
    <repositories>
        <repository>
            <id>wrap-trap.net/maven2/snapshot</id>
            <name>wrap-trap.net Maven Repository</name>
            <url>http://wrap-trap.net/maven2/snapshot</url>
        </repository>
        ...
    <repositories>

## License
MIT: http://rem.mit-license.org
## Summary
FileStoredMap is an implementation of java.util.Map. The Entry(key-value) is stored in files instead of Java heap avoiding the Out Of Memory Error. 
For example, below.
<pre><code>
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
</code></pre>
File format is BSON(http://bsonspec.org/).

## Maven Repository
- Jars: http://wrap-trap.net/maven2/snapshot/net/wrap-trap/collections/FileStoredMap/0.0.1-SNAPSHOT/
- Repository URL: http://wrap-trap.net/maven2/snapshot/
<pre><code>
  &lt;dependencies&gt;
    &lt;dependency&gt;
      &lt;groupId&gt;net.wrap-trap.collections&lt;/groupId&gt;
      &lt;artifactId&gt;FileStoredMap&lt;/artifactId&gt;
      &lt;version&gt;0.0.1-SNAPSHOT&lt;/version&gt;
    &lt;/dependency&gt;
    ...
  &lt;/dependencies&gt;
...
  &lt;repositories&gt;
    &lt;repository&gt;
      &lt;id&gt;wrap-trap.net/maven2/snapshot&lt;/id&gt;
      &lt;name&gt;wrap-trap.net Maven Repository&lt;/name&gt;
      &lt;url&gt;http://wrap-trap.net/maven2/snapshot&lt;/url&gt;
    &lt;/repository&gt;
    ...
  &lt;repositories&gt;
</code></pre>

## License
MIT: http://rem.mit-license.org
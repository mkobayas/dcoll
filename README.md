# dcoll
Distributed collections for JDG

* DistributedOneToMany
* TODO DistributedSearchableList

## DistributedOneToMany

Distributed OneToMany is useful for managing huge size one to many relation data on JDG.
It divide many data into distributed bucket.
This structure is suitable for Distribute Excetution.

```java
class MyOneToMany extends DistributedOneToMany<String, String, String> { // Define your definition.
    public MyOneToMany(Cache cache) { super(cache); }
    public int getMaxBucketSize() { return 500; }
    public int getMinCompactionBucketSize() { return 100; }
    public long getTransferExpireMills() { return 30000; }
}

DefaultCacheManager manager = new ----

AdvancedCache cache = manager.getCache("myDistCache").getAdvancedCache();
MyOneToMany tb = new MyOneToMany(cache);  

cache.lock("FirstKey1");
tb.put("FirstKey1", "SecondKey1-1", "data1-1");
tb.put("FirstKey1", "SecondKey1-2", "data1-2");
tb.put("FirstKey1", "SecondKey1-3", "data1-3");
tb.put("FirstKey1", "SecondKey1-4", "data1-4");
tb.put("FirstKey1", "SecondKey1-5", "data1-5");

cache.lock("FirstKey2");
tb.put("FirstKey2", "SecondKey2-1", "data2-1");
tb.put("FirstKey2", "SecondKey2-2", "data2-2");
tb.put("FirstKey2", "SecondKey2-3", "data2-3");
tb.put("FirstKey2", "SecondKey2-4", "data2-4");
tb.put("FirstKey2", "SecondKey2-5", "data2-5");

FirstKey1 -+- SecondKey1-1 -- data1-1
           +- SecondKey1-2 -- data1-2
           +- SecondKey1-3 -- data1-3
           +- SecondKey1-4 -- data1-4
           +- SecondKey1-5 -- data1-5


FirstKey2 -+- SecondKey2-1 -- data2-1
           +- SecondKey2-2 -- data2-2
           +- SecondKey2-3 -- data2-3
           +- SecondKey2-4 -- data2-4
           +- SecondKey2-5 -- data2-5
```


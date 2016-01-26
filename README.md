# dcoll
Distributed collections for JDG

##

```java
class MyDHMBucket extends DHMBucket<String, String, String> { // Define your DHMBucket definition.
    public TestDHMBucket(Cache cache) { super(cache); }
    public int getMaxBucketSize() { return 500; }
    public int getMinCompactionBucketSize() { return 100; }
    public long getTransferExpireMills() { return 30000; }
}

DefaultCacheManager manager = new ----

AdvancedCache cache = manager.getCache("myDistCache").getAdvancedCache();
MyDHMBucket tb = new MyDHMBucket(cache);  

cache.lock("FirstKey");
tb.put("FirstKey", "SecondKey1", "data1");
tb.put("FirstKey", "SecondKey2", "data1");
tb.put("FirstKey", "SecondKey3", "data1");
tb.put("FirstKey", "SecondKey4", "data1");
tb.put("FirstKey", "SecondKey5", "data1");
```


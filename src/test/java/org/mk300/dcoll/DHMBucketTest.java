/*
 * Copyright 2016 Masazumi Kobayashi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mk300.dcoll;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.thoughtworks.xstream.XStream;

@SuppressWarnings("rawtypes")
public class DHMBucketTest {

    static DefaultCacheManager manager;
    static AdvancedCache cache;

    @BeforeClass
    public static void beforeClass() throws Exception {
        manager = new DefaultCacheManager("infinispan-ut.xml");
        manager.start();
        cache = manager.getCache("testCache").getAdvancedCache();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        manager.stop();
    }

    @Before
    public void clean() {
        cache.clear();
    }

    @Test
    public void testPutDobule() {
        TestDHMBucket tb = new TestDHMBucket(cache);

        for (int i = 0; i < 10000; i++) {
            String pre = tb.put("test1", "valueKey" + i, "data");
            assertNull(pre);
        }

        for (int i = 0; i < 10000; i++) {
            String pre = tb.put("test1", "valueKey" + i, "data");
            assertNotNull(pre);
        }
    }

    @Test
    public void testGetSubKey() {
        TestDHMBucket tb = new TestDHMBucket(cache);

        for (int i = 0; i < 10000; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        for (int i = 0; i < 10000; i++) {
            SubKey subKey = tb.getSubKey("test1", "valueKey" + i);

            DHMBucket.DHMBucketEntry bucket = (DHMBucket.DHMBucketEntry) cache.get(subKey);

            assertTrue(bucket.getValueMap().containsKey("valueKey" + i));
        }
    }

    @Test
    public void testGetSubKeyList() throws Exception {
        TestDHMBucket tb = new TestDHMBucket(cache);

        for (int i = 0; i < 10000; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        // TransferがExpireするのを待つ
        Thread.sleep(tb.getTransferExpireMills() + 1);
        cache.getEvictionManager().processEviction();

        for (int i = 0; i < 10000; i++) {
            List<SubKey> subKeyList = tb.getSubKeyList("test1");

            // 取得したsubKeyでバケットを強制消去消す。
            for (SubKey subKey : subKeyList) {
                cache.remove(subKey);
            }

            // ルートだけがゴミとして残っているはず
            assertEquals(1, cache.size());
        }
    }

    @Test
    public void testIsContains() {
        TestDHMBucket tb = new TestDHMBucket(cache);

        for (int i = 0; i < 100000; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        for (int i = 0; i < 100000; i++) {
            boolean ret = tb.isContains("test1", "valueKey" + i);
            assertTrue(ret);
        }
    }

    @Test
    public void testGetValues() throws Exception {
        TestDHMBucket tb = new TestDHMBucket(cache);

        for (int i = 0; i < 100000; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        Map<String, String> valueMap = tb.getValues("test1");

        assertEquals(100000, valueMap.size());

    }

    @Test
    public void testMultiKey() throws Exception {
        TestDHMBucket tb = new TestDHMBucket(cache);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 10000; i++) {
                tb.put("test" + j, "valueKey" + i, "data");
            }
        }

        for (int j = 0; j < 10; j++) {
            Map<String, String> valueMap = tb.getValues("test" + j);
            assertEquals(10000, valueMap.size());
        }
    }

    @Test
    public void testRemove() throws Exception {
        TestDHMBucket tb = new TestDHMBucket(cache);

        int num = 100000;

        // テストデータ準備
        for (int i = 0; i < num; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        // 削除&戻り値 あり
        for (int i = 0; i < num; i++) {
            boolean ret = tb.remove("test1", "valueKey" + i) != null;
            assertTrue("i=" + i, ret);
        }

        // 削除空振り&戻り値 なし
        for (int i = 0; i < num; i++) {
            boolean ret = tb.remove("test1", "valueKey" + i) != null;
            assertFalse(ret);
        }

        // isContains false
        for (int i = 0; i < num; i++) {
            boolean ret = tb.isContains("test1", "valueKey" + i);
            assertFalse(ret);
        }

        // TransferがExpireするのを待つ
        Thread.sleep(tb.getTransferExpireMills() + 1);
        cache.getEvictionManager().processEviction();

        // ゴミ削除後に0件
        assertEquals(0, cache.size());
    }

    @Test
    public void testPerformance() throws Exception {

        for (int j = 0; j < 10; j++) {
            System.gc();
            TestDHMBucket tb = new TestDHMBucket(cache);

            long start = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                tb.put("test1", "valueKey" + i, "data");
            }
            long end = System.currentTimeMillis();
            System.out.println(end - start + "ms");
        }
    }

    @Test
    public void testHash() throws Exception {
        DHMBucket<String, String, String> tb = new DHMBucket<String, String, String>(cache) {
            public int getMaxBucketSize() {
                return 1000;
            }

            public int getMinCompactionBucketSize() {
                return 500;
            }

            public long getTransferExpireMills() {
                return 500;
            }
        };

        // 100万件 / bucketSize=1000
        for (int i = 0; i < 1000000; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        System.out.println("testHash---");
        List<SubKey> subKeyList = tb.getSubKeyList("test1");
        for (SubKey subKey : subKeyList) {
            Map<String, String> subValueMap = tb.getValuesBySubKey(subKey);
            int size = subValueMap.size();
            System.out.println(size);

            // 各バケットのサイズがgetMaxBucketSizeの1/4以上、1倍以下であること。
            assertTrue(tb.getMaxBucketSize() / 4 < size && size <= tb.getMaxBucketSize());
        }

        // System.out.println(toXML(cache.get("test1")));
    }

    @Test
    public void testTree() throws Exception {
        DHMBucket<String, String, String> tb = new DHMBucket<String, String, String>(cache) {
            public int getMaxBucketSize() {
                return 5;
            }

            public int getMinCompactionBucketSize() {
                return 1;
            }

            public long getTransferExpireMills() {
                return 1;
            }
        };

        for (int i = 0; i < 50; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        Thread.sleep(tb.getTransferExpireMills() + 1);
        cache.getEvictionManager().processEviction();

        System.out.println(toXML(cache.get("test1")));
    }

    /**
     * 同一PUTを10万回でも、ツリー構造が深くならないこと。
     * 
     */
    @Test
    public void testSameAccess100000() {
        DHMBucket<String, String, Integer> tb = new DHMBucket<String, String, Integer>(cache) {
            public int getMaxBucketSize() {
                return 20;
            }

            public int getMinCompactionBucketSize() {
                return 10;
            }

            public long getTransferExpireMills() {
                return 1;
            }
        };

        String key = "test1";
        String valueKey = "hoge";
        Integer value = 0;

        tb.put(key, valueKey, value);

        for (int i = 0; i < 100000; i++) {
            Integer next = tb.get(key, valueKey);
            next = next + 1;
            tb.put(key, valueKey, next);
        }

        DHMBucket.DHMBucketMeta meta = (DHMBucket.DHMBucketMeta) cache.get(key);

        // ルートのLeafの件数が1件
        Assert.assertEquals(1, meta.getTotalSize());

        // 更新されたvalueは10万になっている
        Integer result = (Integer) tb.get(key, valueKey);
        Assert.assertEquals(100000, result.intValue());

        // System.out.println(toXML(root));
        // System.out.println(toXML(bucket));

    }

    /**
     * removeでバケットマージが発生しても、rootのTree構造が管理するBranch,Leafのサイズが正しくなっていること
     */
    @Test
    public void testPut_and_remove_check_BranchSize() throws Exception {
        DHMBucket<String, String, String> tb = new DHMBucket<String, String, String>(cache) {
            public int getMaxBucketSize() {
                return 3;
            }

            public int getMinCompactionBucketSize() {
                return 3;
            }

            public long getTransferExpireMills() {
                return 1;
            }
        };

        String key = "test1";

        // 100個putする
        for (int i = 1; i <= 100; i++) {
            tb.put(key, "dummy" + i, "dummyData" + i);
        }

        // 90個削除する
        for (int i = 6; i <= 100; i++) {
            tb.remove(key, "dummy" + i);
        }
        // ゴミ全部消す
        Thread.sleep(tb.getTransferExpireMills() + 1);
        cache.getEvictionManager().processEviction();

        // XMLで確認
        // Map result = new HashMap();
        // for (Object key2 : cache.keySet()) {
        // result.put(key2, cache.get(key2));
        // }
        // System.out.println(toXML(result));

        // 件数が5件
        DHMBucket.DHMBucketMeta meta = (DHMBucket.DHMBucketMeta) cache.get(key);
        Assert.assertEquals(5, meta.getTotalSize());
    }

    @Test
    public void testRemoveByKey() throws Exception {
        TestDHMBucket tb = new TestDHMBucket(cache);

        int num = 10000;

        // テストデータ準備
        for (int i = 0; i < num; i++) {
            tb.put("test1", "valueKey" + i, "data");
        }

        // 削除&戻り値 true
        boolean ret = tb.remove("test1");
        assertTrue(ret);

        // 削除空振り&戻り値 false
        ret = tb.remove("test1");
        assertFalse(ret);

        // isContains false
        for (int i = 0; i < num; i++) {
            boolean result = tb.isContains("test1", "valueKey" + i);
            assertFalse(result);
        }

        // ゴミ全部消す
        Thread.sleep(tb.getTransferExpireMills() + 1);
        cache.getEvictionManager().processEviction();

        // ゴミ削除後に0件
        assertEquals(0, cache.size());
    }

    /**
     * 同一PUTを2階層目のバケットに10万回PUTしても、rootのBranchのsizeが大きくならない。
     */
    @Test
    public void testSameAccess100000_2Dep() throws Exception {
        DHMBucket<String, String, Integer> tb = new DHMBucket<String, String, Integer>(cache) {
            public int getMaxBucketSize() {
                return 2;
            }

            public int getMinCompactionBucketSize() {
                return 1;
            }

            public long getTransferExpireMills() {
                return 1;
            }
        };

        String key = "test1";

        // 3個putするとバケット分割が発生する。
        tb.put(key, "dummy1", 1);
        tb.put(key, "dummy2", 2);
        tb.put(key, "dummy3", 3);
        Thread.sleep(tb.getTransferExpireMills() + 1);
        cache.getEvictionManager().processEviction();

        // 10万回PUT
        String valueKey = "hoge";
        Integer value = 0;

        tb.put(key, valueKey, value);

        for (int i = 0; i < 100000; i++) {
            Integer next = tb.get(key, valueKey);
            next = next + 1;
            tb.put(key, valueKey, next);
        }

        // XMLで確認
//        Map result = new HashMap();
//        for (Object key2 : cache.keySet()) {
//            result.put(key2, cache.get(key2));
//        }
//        System.out.println(toXML(result));

        DHMBucket.DHMBucketMeta meta = (DHMBucket.DHMBucketMeta) cache.get(key);
        Assert.assertEquals(4, meta.getTotalSize());

        // 更新されたvalueは10万になっている
        Integer result2 = tb.get(key, valueKey);
        Assert.assertEquals(100000, result2.intValue());

    }

    public static <T> String toXML(T obj) {
        XStream xstream = new XStream();
        return xstream.toXML(obj);
    }

    static class TestDHMBucket extends DHMBucket<String, String, String> {

        public TestDHMBucket(Cache cache) {
            super(cache);
        }

        @Override
        public int getMaxBucketSize() {
            return 50;
        }

        @Override
        public int getMinCompactionBucketSize() {
            return 10;
        }

        @Override
        public long getTransferExpireMills() {
            return 100;
        }
    }
}

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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mk300.dcoll.DistributedOneToMany;
import org.mk300.dcoll.SubKey;

/**
 * 更新系メソッドが適切に排他されていると仮定し、排他していない読み取り操作で問題がないかどうかをテストする。
 * 
 * @author mkobayas@redhat.com
 *
 */
@SuppressWarnings("rawtypes")
public class DistributedOneToManyMultiThreadTest {

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

    // 50万件のvalueKeyでテスト
    private static int totalValueKeyNum = 500000;

    @Test
    public void testGetValues() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);

        final String key = "item1";
        final AtomicReference<Exception> error = new AtomicReference<>();

        // put first
        final TestO2M tb = new TestO2M(cache);
        tb.put(key, "valueKey", "data");

        Thread putThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 0;
                while (true) {
                    counter++;
                    tb.put(key, "valueKey" + counter, "data");
                    if (counter == totalValueKeyNum || stop.get()) {
                        stop.set(true);
                        break;
                    }
                    if (counter % (totalValueKeyNum / 10) == 0) {
                        System.out.println("put " + counter);
                    }
                }
            }
        });

        Thread getThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Map<String, String> list = tb.getValues(key);
                        if (list.isEmpty()) {
                            throw new RuntimeException("");
                        }
                        if (stop.get()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    error.set(e);
                    stop.set(true);
                }
            }
        });

        putThread.start();
        getThread.start();

        getThread.join();
        if (error.get() != null) {
            error.get().printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testGet() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);

        final String key = "item1";
        final String valueKey = "valueKey";
        final AtomicReference<Exception> error = new AtomicReference<>();

        // put first
        final TestO2M tb = new TestO2M(cache);
        tb.put(key, valueKey, "data");

        Thread putThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 0;
                while (true) {
                    counter++;
                    tb.put(key, "valueKey" + counter, "data");
                    if (counter == totalValueKeyNum || stop.get()) {
                        stop.set(true);
                        break;
                    }
                    if (counter % (totalValueKeyNum / 10) == 0) {
                        System.out.println("put " + counter);
                    }
                }
            }

        });

        Thread getThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        String value = tb.get(key, valueKey);
                        if (value == null) {
                            throw new RuntimeException("");
                        }
                        if (stop.get()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    error.set(e);
                    stop.set(true);
                }
            }

        });

        putThread.start();
        getThread.start();

        getThread.join();
        if (error.get() != null) {
            error.get().printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testGetValuesBySubKey() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);

        final String key = "item1";
        final String valueKey = "valueKey";
        final AtomicReference<Exception> error = new AtomicReference<>();

        // put first
        final TestO2M tb = new TestO2M(cache);
        tb.put(key, valueKey, "data");

        Thread putThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 0;
                while (true) {
                    counter++;
                    tb.put(key, "valueKey" + counter, "data");
                    if (counter == totalValueKeyNum || stop.get()) {
                        stop.set(true);
                        break;
                    }
                    if (counter % (totalValueKeyNum / 10) == 0) {
                        System.out.println("put " + counter);
                    }
                }
            }

        });

        Thread getThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        List<SubKey> subKeyList = tb.getSubKeyList(key);
                        for (SubKey subKey : subKeyList) {
                            Map<String, String> values = tb.getValuesBySubKey(subKey);
                            if (values == null) {
                                throw new RuntimeException("");
                            }
                        }
                        if (stop.get()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    error.set(e);
                    stop.set(true);
                }
            }

        });

        putThread.start();
        getThread.start();

        getThread.join();
        if (error.get() != null) {
            error.get().printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testIsContains() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);

        final String key = "item1";
        final String valueKey = "valueKey";
        final AtomicReference<Exception> error = new AtomicReference<>();

        // put first
        final TestO2M tb = new TestO2M(cache);
        tb.put(key, valueKey, "data");

        Thread putThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 0;
                while (true) {
                    counter++;
                    tb.put(key, "valueKey" + counter, "data");
                    if (counter == totalValueKeyNum || stop.get()) {
                        stop.set(true);
                        break;
                    }
                    if (counter % (totalValueKeyNum / 10) == 0) {
                        System.out.println("put " + counter);
                    }
                }
            }

        });

        Thread getThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        if (!tb.isContains(key, valueKey)) {
                            throw new RuntimeException("");
                        }
                        if (stop.get()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    error.set(e);
                    stop.set(true);
                }
            }

        });

        putThread.start();
        getThread.start();

        getThread.join();
        if (error.get() != null) {
            error.get().printStackTrace();
            Assert.fail();
        }

    }

    @Test
    public void testRemove() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);

        final String key = "item1";
        final String valueKey = "valueKey";
        final AtomicReference<Exception> error = new AtomicReference<>();

        // put first
        final TestO2M tb = new TestO2M(cache);
        tb.put(key, valueKey, "data");

        Thread putThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long counter = 0;
                int id = 0;
                boolean put = true;
                while (true) {
                    counter++;
                    id++;
                    if (put) {
                        tb.put(key, "valueKey" + id, "data");
                    } else {
                        tb.remove(key, "valueKey" + id);
                    }

                    if (counter == totalValueKeyNum || stop.get()) {
                        stop.set(true);
                        break;
                    }

                    if (counter % (totalValueKeyNum / 100) == 0) {
                        if (put) {
                            System.out.println("put " + counter);
                        } else {
                            System.out.println("remove " + counter);
                        }
                        put = put ? false : true;
                        id = 0;
                        cache.getEvictionManager().processEviction();
                    }
                }
            }

        });

        Thread getThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        if (!tb.isContains(key, valueKey)) {
                            throw new RuntimeException("");
                        }
                        if (stop.get()) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    error.set(e);
                    stop.set(true);
                }
            }

        });

        putThread.start();
        getThread.start();

        getThread.join();
        if (error.get() != null) {
            error.get().printStackTrace();
            Assert.fail();
        }

    }

    static class TestO2M extends DistributedOneToMany<String, String, String> {

        public TestO2M(Cache cache) {
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

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;

/**
 * Distribute one to many bucket<br>
 * バケット構造を第一キー(key)と第二キー(valueKey)で管理する。
 * 1つのkeyに対して大量のvalueKeyを関連付けることができる。
 * 各バケットはvalueKeyのハッシュ値を元に分散バケットで管理されるため、
 * valueKeyが大量になったとしても、一定のコストで更新処理を行うことがでる。
 * DistExecによる分散処理で特定のkeyに紐づく全てのvalueを処理するケースで
 * 最大の性能がでる方式である。<br>
 * 
 * @author mkobayas@redhat.com
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
abstract public class DistributedOneToMany<K, VK, V> {

    abstract public int getMaxBucketSize();

    abstract public int getMinCompactionBucketSize();

    abstract public long getTransferExpireMills();

    private AdvancedCache cache;

    public DistributedOneToMany(Cache cache) {
        this.cache = (AdvancedCache) cache;
    }

    /**
     * 指定されたkeyとvalueKeyでvalueをキャッシュする。<br>
     * value追加により対象となるバケットが{@link #getMaxBucketSize()}を超えた時は、 バケットの分割が発生する。<br>
     * この時、このキャッシュに対する読み取り操作の同時アクセスを可能とするためにデータ移行用のvalue(
     * {@link BucketTrasfer} が putされる。<br>
     * このBucketTrasferは、バケット分割前のsubKeyでバケット分割後のデータを読み取るために用いられる。<br>
     * <b>必ずkeyでクラスタワイドロックを取得すること。</b>
     * 
     * @param key 第一キー
     * @param valueKey 第二キー
     * @param value 保持するvalue
     * @return 前回の値があれば、その値。無ければnull。
     */
    public V put(K key, VK valueKey, V value) {

        BucketMeta meta = (BucketMeta) cache.get(key);

        if (meta == null) {
            // 新規キー
            meta = new BucketMeta();
            meta.setBucketSize(0, 1);

            BucketEntry bucket = new BucketEntry();
            bucket.getValueMap().put(valueKey, value);

            cache.put(meta.getSubKey(0), bucket);
            cache.put(key, meta);

            return null;
        }

        // System.out.println(meta);
        // バケットインデックス特定
        int hash = HashFunction.hash(valueKey);
        int index = meta.getIndex(hash);
        // System.out.println(hash + "->" + index);

        SubKey subkey = meta.getSubKey(index);

        // バケット取得
        BucketEntry<K, VK, V> bucket = (BucketEntry) cache.get(subkey);
        
        if(bucket == null) {
            bucket = new BucketEntry<>();
        }
        
        if (bucket.getValueMap().containsKey(valueKey)) {
            // 既にvalueKeyが存在　-> valueを差し替えるだけでルートの構造は変化しない。
            V preValue = bucket.getValueMap().put(valueKey, value);
            cache.put(subkey, bucket);
            return preValue;
        }

        int bucketSize = meta.getBuketSize(index);
        if (bucketSize < getMaxBucketSize()) {
            // 新規valueKey、且つ、バケット容量上限以内 -> バケットにvalue追加。
            bucket.getValueMap().put(valueKey, value);
            meta.setBucketSize(index, bucket.getValueMap().size());
            cache.put(subkey, bucket);
            cache.put(key, meta);
            return null;
        }

        // バケット分割
        meta = new BucketMeta(meta); // immutable
        meta.splitDown(index);
        int leftIndex = index;
        int rightIndex = index + 1;
        int splitPoint = meta.getHashRnge(rightIndex)[0];

        BucketEntry newLeftBucket = new BucketEntry();
        BucketEntry newRightBucket = new BucketEntry();

        bucket.getValueMap().put(valueKey, value);
        for (Entry<VK, V> entry : bucket.getValueMap().entrySet()) {
            VK entryKey = entry.getKey();
            V entryValue = entry.getValue();

            int entryKeyHash = HashFunction.hash(entryKey);
            if (entryKeyHash < splitPoint) {
                newLeftBucket.getValueMap().put(entryKey, entryValue);
            } else {
                newRightBucket.getValueMap().put(entryKey, entryValue);
            }
        }
        meta.setBucketSize(leftIndex, newLeftBucket.getValueMap().size());
        meta.setBucketSize(rightIndex, newRightBucket.getValueMap().size());

        // 更新 U1-U4までの実行順序が非常に重要
        SubKey leftSubKey = meta.getSubKey(leftIndex);
        SubKey RitghtSubKey = meta.getSubKey(rightIndex);

        // put new bucket(U1: order is important)
        cache.put(leftSubKey, newLeftBucket);
        cache.put(RitghtSubKey, newRightBucket);

        // このタイミングでは、新旧バケットがキャッシュ上に存在する。
        // 但し、ローカルのバケットルートもリモートのバケットルートも旧バケットを指し示している。

        // transfer (U2: order is important)
        // 古いバケットは削除し、その代わりにデータ移行リンク情報を投入。
        // データ移行リンク情報は直ぐにゴミになるのでガベージが必要。現状ではgarbageLocalTransferEntry()をJDGクラスタの各ノードで
        // で実行することによりゴミ削除する想定。
        BucketTrasfer tbt = new BucketTrasfer();
        tbt.getTransSubKeyList().add(leftSubKey);
        tbt.getTransSubKeyList().add(RitghtSubKey);
        cache.put(subkey, tbt, getTransferExpireMills(), TimeUnit.MILLISECONDS);

        // この時点で旧バケットは消えているが、上記のデータ移行リンクにより、古いsubKeyでのアクセス時に
        // 新バケットに誘導可能となっている。

        // update (U4: order is important)
        cache.put(key, meta);

        return null;
    }

    /**
     * 指定されたkey、valueKeyを元に、そのvalueKeyが存在している可能性があるBucketのsubKeyを特定する。<br>
     * このメソッドはvalueKeyのハッシュ計算に基いてsubkeyを特定するため高速だが、そのvalueKeyが本当に存在しているかはチェックしない
     * 。<br>
     * 
     * @param key 第一キー
     * @param valueKey 第二キー
     * @return 指定されたKeyが存在しない場合はnull
     */
    public SubKey getSubKey(K key, VK valueKey) {
        BucketMeta meta = (BucketMeta) cache.get(key);
        if (meta == null) {
            return null;
        }

        int hash = HashFunction.hash(valueKey);
        int index = meta.getIndex(hash);
        return meta.getSubKey(index);
    }

    /**
     * 指定されたkeyとvalueKeyに対応するvalueを削除する。<br>
     * この操作の結果、第一キーにぶら下がる第二キーが0件になった場合は、第一キーごと削除する。<br>
     * また、この操作の結果、対象バケットとその隣接バケットの合計データ件数が
     * {@link #getMinCompactionBucketSize()}
     * 以下になった場合は、その対象バケットの隣接バケットと統合を実施する。<br>
     * バケット統合の際に、このキャッシュに対する読み取り操作の同時アクセスを可能とするためにデータ移行用のvalue(
     * {@link BucketTrasfer} が putされる。<br>
     * このBucketTrasferは、バケット統合前のsubKeyでバケット統合後のデータを読み取るために用いられる。<br>
     * <b>必ずkeyでクラスタワイドロックを取得すること。</b>
     * 
     * @param key 第一キー
     * @param valueKey 第二キー
     * @return 前回の値があれば、その値。無ければnull。
     */
    public V remove(K key, VK valueKey) {
        BucketMeta meta = (BucketMeta) cache.get(key);
        if (meta == null) {
            return null;
        }

        int hash = HashFunction.hash(valueKey);
        int index = meta.getIndex(hash);
        SubKey subKey = meta.getSubKey(index);

        BucketEntry<K, VK, V> bucket = (BucketEntry) cache.get(subKey);

        if (bucket == null || !bucket.getValueMap().containsKey(valueKey)) {
            return null;
        }

        V pre = bucket.getValueMap().remove(valueKey);

        // set decremented size
        int bucketSize = meta.setBucketSize(index, bucket.getValueMap().size());

        if (meta.getTotalSize() <= 0) {
            // completely delete
            remove(key);
            return pre;
        }

        if (meta.bucketNum() <= 1) {
            // simple remove case
            cache.put(subKey, bucket);
            cache.put(key, meta);
            return pre;
        }

        // size check
        int anotherIndex;
        int anotherBucketSize;

        if (index == 0) {
            anotherIndex = index + 1;
        } else if (index == meta.bucketNum() - 1) {
            anotherIndex = index - 1;
        } else {
            // 左右のバケットの内、小さい方を合併候補とする。
            if (meta.getBuketSize(index - 1) < meta.getBuketSize(index + 1)) {
                anotherIndex = index - 1;
            } else {
                anotherIndex = index + 1;
            }
        }
        anotherBucketSize = meta.getBuketSize(anotherIndex);

        if (bucketSize + anotherBucketSize > getMinCompactionBucketSize()) {
            // simple remove case
            cache.put(subKey, bucket);
            cache.put(key, meta);
            return pre;
        }

        // merge up
        SubKey anotherSubKey = meta.getSubKey(anotherIndex);
        BucketEntry<K, VK, V> anotherBucket = (BucketEntry) cache.get(anotherSubKey);

        int newIndex = Math.min(index, anotherIndex);

        meta = new BucketMeta(meta); // immutable;
        meta.mergeUp(newIndex);

        SubKey newSubKey = meta.getSubKey(newIndex);

        // new merged bucket
        BucketEntry newBucket = new BucketEntry();
        newBucket.getValueMap().putAll(bucket.getValueMap());
        newBucket.getValueMap().putAll(anotherBucket.getValueMap());
        meta.setBucketSize(newIndex, newBucket.getValueMap().size());

        // transfer link
        BucketTrasfer tbt = new BucketTrasfer();
        tbt.getTransSubKeyList().add(newSubKey);

        cache.put(newSubKey, newBucket);
        cache.put(subKey, tbt, getTransferExpireMills(), TimeUnit.MILLISECONDS);
        cache.put(anotherSubKey, tbt, getTransferExpireMills(), TimeUnit.MILLISECONDS);
        cache.put(key, meta);

        return pre;
    }

    /**
     * 指定されたkeyにぶら下がる全てのvalueKeyとvalueのMapを取得する(高コスト: バケットの数に比例する)
     * 
     * @param key 第一キー
     * @return keyにぶら下がる全てのvalueKeyとvalue
     */
    public Map<VK, V> getValues(K key) {
        Map<VK, V> values = new HashMap<>();
        BucketMeta meta = (BucketMeta) cache.get(key);
        if (meta != null) {
            List<SubKey> subKeyList = meta.getSubKeyList();
            for (SubKey subkey : subKeyList) {
                BucketEntry bucket = getBucket(subkey);
                if (bucket != null) {
                    values.putAll((Map<VK, V>) bucket.getValueMap());
                }
            }
        }

        return values;

    }

    /**
     * 指定されたsubKeyにぶら下がるvalueKeyとvalueのMapのみ取得する。(低コスト)
     * 
     * @param subKey バケットのサブキー
     * @return バケットに含まれる全てのvalueKeyとvalue
     */
    public Map<VK, V> getValuesBySubKey(SubKey subKey) {
        Map<VK, V> values = new HashMap<>();

        BucketEntry<K, VK, V> bucket = getBucket(subKey);
        if (bucket != null) {
            values.putAll(bucket.getValueMap());
        }

        return values;
    }

    /**
     * 指定されたkeyにぶら下がるバケットのsubKeyのリストを取得する(低コスト)
     * 
     * @param key 第一キー
     * @return バケットのサブキーのリスト
     */
    public List<SubKey> getSubKeyList(K key) {
        BucketMeta meta = (BucketMeta) cache.get(key);
        if (meta != null) {
            return meta.getSubKeyList();
        } else {
            return new ArrayList<>(0);
        }
    }

    /**
     * 指定されたキーに、指定されたvalueKeyが存在するかどうかを判定する。
     * 
     * @param key 第一キー
     * @param valueKey 第二キー
     * @return valueKeyが含まれている場合はtrue
     */
    public boolean isContains(K key, VK valueKey) {
        BucketMeta meta = (BucketMeta) cache.get(key);
        if (meta == null) {
            return false;
        }

        int hash = HashFunction.hash(valueKey);
        int index = meta.getIndex(hash);

        BucketEntry<K, VK, V> bucket = getBucket(meta.getSubKey(index));
        if (bucket != null && bucket.getValueMap().containsKey(valueKey)) {
            return true;
        }

        return false;
    }

    /**
     * 指定されたkeyとvalueKeyでvalueを取得する。
     * 
     * @param key 第一キー
     * @param valueKey 第二キー
     * @return value
     */
    public V get(K key, VK valueKey) {
        BucketMeta meta = (BucketMeta) cache.get(key);
        if (meta == null) {
            return null;
        }

        int hash = HashFunction.hash(valueKey);
        int index = meta.getIndex(hash);

        BucketEntry<K, VK, V> bucket = getBucket(meta.getSubKey(index));
        if (bucket != null) {
            return bucket.getValueMap().get(valueKey);
        } else {
            return null;
        }
    }

    /**
     * BucketTrasferを考慮して、指定されたバケットを取得する。
     * 
     * @param subKey バケットのサブキー
     * @return バケット
     */
    private BucketEntry getBucket(SubKey subKey) {
        Object tmp = cache.get(subKey);
        if (tmp instanceof BucketEntry) {
            return (BucketEntry) tmp;
        } else if (tmp instanceof BucketTrasfer) {
            BucketEntry mergeBucket = new BucketEntry();
            BucketTrasfer trans = (BucketTrasfer) tmp;
            for (SubKey transSubKey : trans.getTransSubKeyList()) {
                BucketEntry actualBucket = getBucket(transSubKey);
                mergeBucket.getValueMap().putAll(actualBucket.getValueMap());
            }

            return mergeBucket;
        }

        return null;
    }

    /**
     * key指定で削除する
     * 
     * @param key 第一キー
     */
    public boolean remove(K key) {
        for (SubKey subKey : getSubKeyList(key)) {
            cache.remove(subKey);
        }
        return cache.remove(key) != null;
    }

    public static class BucketMeta implements Serializable {

        private static final long serialVersionUID = 1L;

        private int[] minHash;
        private int[] bucketSize;
        private long[] subkey;

        /**
         * Constructor
         */
        public BucketMeta() {
            minHash = new int[1];
            minHash[0] = Integer.MIN_VALUE;

            bucketSize = new int[1];
            bucketSize[0] = 0;

            SubKey sKey = new SubKey();
            subkey = new long[2];
            subkey[0] = sKey.getUnique();
            subkey[1] = sKey.getCounter();
        }

        /**
         * Copy constructor
         * 
         * @param origin
         */
        public BucketMeta(BucketMeta origin) {
            minHash = new int[origin.minHash.length];
            System.arraycopy(origin.minHash, 0, minHash, 0, origin.minHash.length);

            bucketSize = new int[origin.bucketSize.length];
            System.arraycopy(origin.bucketSize, 0, bucketSize, 0, origin.bucketSize.length);

            subkey = new long[origin.subkey.length];
            System.arraycopy(origin.subkey, 0, subkey, 0, origin.subkey.length);
        }

        public int getIndex(int hash) {
            int index = 1;
            for (; index < minHash.length; index++) {
                if (hash < minHash[index]) {
                    return index - 1;
                }
            }
            return minHash.length - 1;
        }

        public SubKey getSubKey(int index) {
            long s1 = subkey[index * 2];
            long s2 = subkey[index * 2 + 1];

            return new SubKey(s1, s2);
        }

        public int getBuketSize(int index) {
            return bucketSize[index];
        }

        public int setBucketSize(int index, int size) {
            bucketSize[index] = size;
            return bucketSize[index];
        }

        public int[] getHashRnge(int index) {
            if (index == minHash.length - 1) {
                return new int[] { minHash[index], Integer.MAX_VALUE };
            } else {
                return new int[] { minHash[index], minHash[index + 1] - 1 };
            }
        }

        public void splitDown(int index) {
            int[] oldMinHash = minHash;
            int[] oldBucketSize = bucketSize;
            long[] oldSubkey = subkey;

            minHash = new int[minHash.length + 1];
            bucketSize = new int[bucketSize.length + 1];
            subkey = new long[subkey.length + 2];

            if (index == 0) {
                // [A, B, C] -> [ A1, A2, B, C]
                System.arraycopy(oldMinHash, 1, minHash, 2, oldMinHash.length - 1);
                System.arraycopy(oldBucketSize, 1, bucketSize, 2, oldBucketSize.length - 1);
                System.arraycopy(oldSubkey, 2, subkey, 4, oldSubkey.length - 2);

            } else if (index == oldMinHash.length - 1) {
                // [A, B, C] -> [ A, B, C1, C2]
                System.arraycopy(oldMinHash, 0, minHash, 0, oldMinHash.length - 1);
                System.arraycopy(oldBucketSize, 0, bucketSize, 0, oldBucketSize.length - 1);
                System.arraycopy(oldSubkey, 0, subkey, 0, oldSubkey.length - 2);

            } else {
                // [A, B, C] -> [ A, B1, B2, C]
                System.arraycopy(oldMinHash, 0, minHash, 0, index);
                System.arraycopy(oldMinHash, index + 1, minHash, index + 2, oldMinHash.length - index - 1);

                System.arraycopy(oldBucketSize, 0, bucketSize, 0, index);
                System.arraycopy(oldBucketSize, index + 1, bucketSize, index + 2, oldBucketSize.length - index - 1);

                System.arraycopy(oldSubkey, 0, subkey, 0, index * 2);
                System.arraycopy(oldSubkey, (index + 1) * 2, subkey, (index + 2) * 2, oldSubkey.length - (index + 1) * 2);
            }

            minHash[index] = oldMinHash[index];
            if (index == oldMinHash.length - 1) {
                minHash[index + 1] = oldMinHash[index] / 2 + Integer.MAX_VALUE / 2;
            } else {
                minHash[index + 1] = oldMinHash[index] / 2 + oldMinHash[index + 1] / 2;
            }
            bucketSize[index] = 0;
            bucketSize[index + 1] = 0;

            SubKey sKey1 = new SubKey();
            subkey[index * 2] = sKey1.getUnique();
            subkey[index * 2 + 1] = sKey1.getCounter();

            SubKey sKey2 = new SubKey();
            subkey[index * 2 + 2] = sKey2.getUnique();
            subkey[index * 2 + 3] = sKey2.getCounter();
        }

        public void mergeUp(int index) {

            int[] oldMinHash = minHash;
            int[] oldBucketSize = bucketSize;
            long[] oldSubkey = subkey;

            minHash = new int[minHash.length - 1];
            bucketSize = new int[bucketSize.length - 1];
            subkey = new long[subkey.length - 2];

            if (index == 0) {
                if (oldMinHash.length <= 2) {
                    // [A1, A2] -> [A]
                } else {
                    // [A1, A2, B, C] -> [ A, B, C]
                    System.arraycopy(oldMinHash, 2, minHash, 1, oldMinHash.length - 2);
                    System.arraycopy(oldBucketSize, 2, bucketSize, 1, oldBucketSize.length - 2);
                    System.arraycopy(oldSubkey, 4, subkey, 2, oldSubkey.length - 4);
                }
            } else if (index >= oldMinHash.length - 2) {
                // [ A, B, C1, C2] -> [A, B, C]
                System.arraycopy(oldMinHash, 0, minHash, 0, oldMinHash.length - 2);
                System.arraycopy(oldBucketSize, 0, bucketSize, 0, oldBucketSize.length - 2);
                System.arraycopy(oldSubkey, 0, subkey, 0, oldSubkey.length - 4);

            } else {
                // [ A, B1, B2, C] -> [A, B, C]
                System.arraycopy(oldMinHash, 0, minHash, 0, index);
                System.arraycopy(oldMinHash, index + 2, minHash, index + 1, oldMinHash.length - index - 2);

                System.arraycopy(oldBucketSize, 0, bucketSize, 0, index);
                System.arraycopy(oldBucketSize, index + 2, bucketSize, index + 1, oldBucketSize.length - index - 2);

                System.arraycopy(oldSubkey, 0, subkey, 0, (index) * 2);
                System.arraycopy(oldSubkey, (index + 2) * 2, subkey, (index + 1) * 2, oldSubkey.length - (index + 2) * 2);
            }

            minHash[index] = oldMinHash[index];
            bucketSize[index] = 0;

            SubKey sKey = new SubKey();
            subkey[index * 2] = sKey.getUnique();
            subkey[index * 2 + 1] = sKey.getCounter();

        }

        public List<SubKey> getSubKeyList() {
            List<SubKey> subKeyList = new ArrayList<>(subkey.length / 2);
            for (int i = 0; i < subkey.length - 1; i = i + 2) {
                subKeyList.add(new SubKey(subkey[i], subkey[i + 1]));
            }
            return subKeyList;
        }

        public long getTotalSize() {
            long sum = 0;
            for (int size : bucketSize) {
                sum += (long) size;
            }
            return sum;
        }

        public int bucketNum() {
            return minHash.length;
        }

        @Override
        public String toString() {
            return "DHMBMeta [minHash=" + Arrays.toString(minHash) + ", bucketSize=" + Arrays.toString(bucketSize) + ", subkey=" + Arrays.toString(subkey) + "]";
        }
    }

    public static class BucketEntry<K, VK, V> implements Serializable {
        private static final long serialVersionUID = 1L;
        private Map<VK, V> valueMap = new ConcurrentHashMap<>();

        public Map<VK, V> getValueMap() {
            return valueMap;
        }
    }

    public static class BucketTrasfer implements Serializable {
        private static final long serialVersionUID = 1L;
        private List<SubKey> transSubKeyList = new ArrayList<>();

        public List<SubKey> getTransSubKeyList() {
            return transSubKeyList;
        }

        public void setTransSubKeyList(List<SubKey> transSubKeyList) {
            this.transSubKeyList = transSubKeyList;
        }
    }

    public static class HashFunction {
        public static int hash(Object valueKey) {
            int hashcode = valueKey.hashCode();

            byte b0 = (byte) hashcode;
            byte b1 = (byte) (hashcode >>> 8);
            byte b2 = (byte) (hashcode >>> 16);
            byte b3 = (byte) (hashcode >>> 24);
            State state = new State();

            state.h1 = 0x9368e53c2f6af274L ^ 9001;
            state.h2 = 0x586dcd208f7cd3fdL ^ 9001;

            state.c1 = 0x87c37b91114253d5L;
            state.c2 = 0x4cf5ad432745937fL;

            state.k1 = 0;
            state.k2 = 0;

            state.k1 ^= (long) b3 << 24;
            state.k1 ^= (long) b2 << 16;
            state.k1 ^= (long) b1 << 8;
            state.k1 ^= b0;
            bmix(state);

            state.h2 ^= 4;

            state.h1 += state.h2;
            state.h2 += state.h1;

            state.h1 = fmix(state.h1);
            state.h2 = fmix(state.h2);

            state.h1 += state.h2;
            state.h2 += state.h1;

            int ret = (int) (state.h1 >>> 32);
            return ret;
        }

        private static void bmix(State state) {
            state.k1 *= state.c1;
            state.k1 = (state.k1 << 23) | (state.k1 >>> 64 - 23);
            state.k1 *= state.c2;
            state.h1 ^= state.k1;
            state.h1 += state.h2;

            state.h2 = (state.h2 << 41) | (state.h2 >>> 64 - 41);

            state.k2 *= state.c2;
            state.k2 = (state.k2 << 23) | (state.k2 >>> 64 - 23);
            state.k2 *= state.c1;
            state.h2 ^= state.k2;
            state.h2 += state.h1;

            state.h1 = state.h1 * 3 + 0x52dce729;
            state.h2 = state.h2 * 3 + 0x38495ab5;

            state.c1 = state.c1 * 5 + 0x7b7d159c;
            state.c2 = state.c2 * 5 + 0x6bce6396;
        }

        private static long fmix(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }

        private static class State {
            long h1, h2, k1, k2, c1, c2;
        }
    }
}

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
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * バケット管理用のユニークキー
 * 
 * @author mkobayas@redhat.com
 *
 */
public class SubKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private static long uniqueHolder = new SecureRandom().nextLong();
    private static AtomicLong counterHolder = new AtomicLong();

    private final long unique;
    private final long counter;

    public SubKey() {
        unique = uniqueHolder;
        counter = counterHolder.incrementAndGet();
    }

    public SubKey(long unique, long counter) {
        this.unique = unique;
        this.counter = counter;
    }

    public long getUnique() {
        return unique;
    }

    public long getCounter() {
        return counter;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toHexString(unique)).append(":").append(Long.toHexString(counter));
        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (counter ^ (counter >>> 32));
        result = prime * result + (int) (unique ^ (unique >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubKey other = (SubKey) obj;
        if (counter != other.counter)
            return false;
        if (unique != other.unique)
            return false;
        return true;
    }

}

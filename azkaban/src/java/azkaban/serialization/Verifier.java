/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.serialization;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class Verifier
{
    public static final void verifyKeysExist(Map<String, Object> descriptor, String... keys)
    {
        List<String> nonExistentKeys = new ArrayList<String>();

        for (String key : keys) {
            if (! descriptor.containsKey(key)) {
                nonExistentKeys.add(key);
            }
        }

        if (! nonExistentKeys.isEmpty()) {
            throw new RuntimeException(
                    String.format("Keys[%s] are required in descriptor[%s]", nonExistentKeys, descriptor)
            );
        }
    }

    public static final <T> T getVerifiedObject(Map<String, Object> descriptor, String key, Class<T> expectedClass)
    {
        Object obj = descriptor.get(key);

        if (obj == null) {
            throw new RuntimeException(
                    String.format("Key[%s] is required as a %s on descriptor[%s]", key, expectedClass, descriptor)
            );
        }

        if (expectedClass.isAssignableFrom(obj.getClass())) {
            return expectedClass.cast(obj);
        }
        else {
            throw new RuntimeException(
                    String.format(
                            "Key[%s] was a %s, but should have been a %s on descriptor[%s]",
                            key,
                            obj.getClass(),
                            expectedClass,
                            descriptor
                    )
            );
        }
    }

    public static final <T> T getOptionalObject(Map<String, Object> descriptor, String key, Class<T> expectedClass)
    {
        Object obj = descriptor.get(key);

        if (obj == null) {
            return null;
        }

        if (expectedClass.isAssignableFrom(obj.getClass())) {
            return expectedClass.cast(obj);
        }
        else {
            throw new RuntimeException(
                    String.format(
                            "Key[%s] was a %s, but should have been a %s on descriptor[%s]",
                            key,
                            obj.getClass(),
                            expectedClass,
                            descriptor
                    )
            );
        }
    }

    public static final <T> T getVerifiedObject(List<Object> descriptor, int index, Class<T> expectedClass)
    {
        if (index < 0) {
            throw new RuntimeException("How about a non-negative index?  Got " + index);
        }

        if (descriptor.size() >= index) {
            throw new RuntimeException(
                    String.format("Index[%s] is too large for list[%s]", index, descriptor)
            );
        }

        Object obj = descriptor.get(index);

        if (expectedClass.isAssignableFrom(obj.getClass())) {
            return expectedClass.cast(obj);
        }
        else {
            throw new RuntimeException(
                    String.format(
                            "Object at index[%s] was a %s, but should have been a %s on list[%s]",
                            index,
                            obj.getClass(),
                            expectedClass,
                            descriptor
                    )
            );
        }
    }

    public static final boolean equals(Object lhs, Object rhs)
    {
        if (lhs == null || rhs == null) {
            return rhs == lhs;
        }
        if (lhs instanceof Map && rhs instanceof Map) {
            return equals((Map) lhs, (Map) rhs);
        }
        else if (lhs instanceof List && rhs instanceof List) {
            return equals((List) lhs, (List) rhs);
        }
        else {
            return lhs.toString().equals(rhs.toString());
        }
    }

    public static final boolean equals(Map<String, Object> lhs, Map<String, Object> rhs)
    {
        if (lhs.size() != rhs.size()) {
            return false;
        }

        for (Map.Entry<String, Object> entry : lhs.entrySet()) {
            if (! rhs.containsKey(entry.getKey())) {
                return false;
            }

            if (! equals(entry.getValue(), rhs.get(entry.getKey()))) {
                return false;
            }
        }

        return true;
    }

    public static final boolean equals(List<Object> lhs, List<Object> rhs)
    {
        if (lhs.size() != rhs.size()) {
            return false;
        }

        final Iterator<Object> lhsIter = lhs.iterator();
        final Iterator<Object> rhsIter = rhs.iterator();
        while (lhsIter.hasNext()) {
            if (! equals(lhsIter.next(), rhsIter.next())) {
                return false;
            }
        }

        return true;
    }

    public static String getString(Map<String, Object> descriptor, String key)
    {
        Object obj = descriptor.get(key);

        if (obj == null) {
            throw new RuntimeException(
                    String.format("Key[%s] is required as a String on descriptor[%s]", key, descriptor)
            );
        }

        return obj.toString();
    }

    public static DateTime getOptionalDateTime(Map<String, Object> descriptor, String key)
    {
        Object obj = descriptor.get(key);

        if (obj == null) {
            return null;
        }

        return new DateTime(obj.toString());
    }

    public static <T extends Enum> T getEnumType(Map<String, Object> descriptor, String key, Class<T> enumType)
    {
        String enumString = getString(descriptor, key);

        try {
            return (T) Enum.valueOf(enumType, enumString);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(String.format(
                    "Unknown enumeration value[%s] on enum[%s].",
                    enumString,
                    enumType
            ));
        }
    }
}

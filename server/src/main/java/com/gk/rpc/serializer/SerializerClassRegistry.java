/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.gk.rpc.serializer;

import com.gk.rpc.netty.MergedWarpMessage;
import com.gk.rpc.protocol.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provide a unified serialization registry, this class used for {@code seata-serializer-fst}
 * @author funkye
 */
public class SerializerClassRegistry {

    private static final Map<Class<?>, Object> REGISTRATIONS = new LinkedHashMap<>();

    static {

        // register commonly class
        registerClass(HashMap.class);
        registerClass(ArrayList.class);
        registerClass(LinkedList.class);
        registerClass(HashSet.class);
        registerClass(TreeSet.class);
        registerClass(Hashtable.class);
        registerClass(Date.class);
        registerClass(Calendar.class);
        registerClass(ConcurrentHashMap.class);
        registerClass(SimpleDateFormat.class);
        registerClass(GregorianCalendar.class);
        registerClass(Vector.class);
        registerClass(BitSet.class);
        registerClass(StringBuffer.class);
        registerClass(StringBuilder.class);
        registerClass(Object.class);
        registerClass(Object[].class);
        registerClass(String[].class);
        registerClass(byte[].class);
        registerClass(char[].class);
        registerClass(int[].class);
        registerClass(float[].class);
        registerClass(double[].class);


        registerClass(MergedWarpMessage.class);
        registerClass(MergeResultMessage.class);
        registerClass(RegisterRMRequest.class);
        registerClass(RegisterRMResponse.class);
        registerClass(RegisterTMRequest.class);
        registerClass(RegisterTMResponse.class);
    }
    
    /**
     * only supposed to be called at startup time
     *
     * @param clazz object type
     */
    public static void registerClass(Class<?> clazz) {
        registerClass(clazz, null);
    }

    /**
     * only supposed to be called at startup time
     *
     * @param clazz object type
     * @param serializer object serializer
     */
    public static void registerClass(Class<?> clazz, Object serializer) {
        if (clazz == null) {
            throw new IllegalArgumentException("Class registered cannot be null!");
        }
        REGISTRATIONS.put(clazz, serializer);
    }

    /**
     * get registered classes
     *
     * @return class serializer
     * */
    public static Map<Class<?>, Object> getRegisteredClasses() {
        return REGISTRATIONS;
    }
}

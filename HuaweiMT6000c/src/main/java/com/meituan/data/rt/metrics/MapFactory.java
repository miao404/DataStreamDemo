package com.meituan.data.rt.metrics;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.lang.reflect.Type;
import java.util.Map;

public class MapFactory<K, V> extends TypeInfoFactory<Map<K, V>> {
    public MapFactory() {
    }

    public TypeInformation<Map<K, V>> createTypeInfo(Type type, Map<String, TypeInformation<?>> map) {
        TypeInformation<?> keyType = (TypeInformation)map.get("K");
        TypeInformation<?> valueType = (TypeInformation)map.get("V");
        return new MapTypeInfo(keyType, valueType);
    }
}


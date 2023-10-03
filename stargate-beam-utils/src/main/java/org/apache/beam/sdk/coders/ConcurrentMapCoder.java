package org.apache.beam.sdk.coders;

import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("deprecation")
public class ConcurrentMapCoder<K, V> extends StructuredCoder<ConcurrentHashMap<K, V>> {
    private MapCoder<K, V> mapCoder;

    private ConcurrentMapCoder(final Coder<K> keyCoder, final Coder<V> valueCoder) {
        this.mapCoder = MapCoder.of(keyCoder, valueCoder);
    }

    public static <K, V> ConcurrentMapCoder<K, V> of(final Coder<K> keyCoder, final Coder<V> valueCoder) {
        return new ConcurrentMapCoder<>(keyCoder, valueCoder);
    }

    @Override
    public void encode(final ConcurrentHashMap<K, V> map, final OutputStream outStream) throws CoderException, IOException {
        mapCoder.encode(map, outStream);
    }

    @Override
    public ConcurrentHashMap<K, V> decode(final InputStream inStream) throws CoderException, IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public ConcurrentHashMap<K, V> decode(final InputStream inStream, final Context context) throws IOException, CoderException {
        int size = BitConverters.readBigEndianInt(inStream);
        if (size == 0) {
            return new ConcurrentHashMap<>();
        }

        ConcurrentHashMap<K, V> retval = new ConcurrentHashMap<>(size);
        for (int i = 0; i < size - 1; ++i) {
            K key = mapCoder.getKeyCoder().decode(inStream);
            V value = mapCoder.getValueCoder().decode(inStream);
            retval.put(key, value);
        }

        K key = mapCoder.getKeyCoder().decode(inStream);
        V value = mapCoder.getValueCoder().decode(inStream, context);
        retval.put(key, value);
        return retval;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return mapCoder.getCoderArguments();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        mapCoder.verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
        return mapCoder.consistentWithEquals();
    }

    @Override
    public Object structuralValue(final ConcurrentHashMap<K, V> value) {
        if (consistentWithEquals()) {
            return value;
        } else {
            Map<Object, Object> ret = new ConcurrentHashMap<>(value.size());
            for (Map.Entry<K, V> entry : value.entrySet()) {
                ret.put(mapCoder.getKeyCoder().structuralValue(entry.getKey()), mapCoder.getValueCoder().structuralValue(entry.getValue()));
            }
            return ret;
        }
    }

    @Override
    public void registerByteSizeObserver(final ConcurrentHashMap<K, V> map, final ElementByteSizeObserver observer) throws Exception {
        mapCoder.registerByteSizeObserver(map, observer);
    }

    @Override
    public TypeDescriptor<ConcurrentHashMap<K, V>> getEncodedTypeDescriptor() {
        return new TypeDescriptor<ConcurrentHashMap<K, V>>() {
        }.where(new TypeParameter<>() {
        }, mapCoder.getKeyCoder().getEncodedTypeDescriptor()).where(new TypeParameter<V>() {
        }, mapCoder.getValueCoder().getEncodedTypeDescriptor());
    }
}

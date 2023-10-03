package org.apache.beam.sdk.io.kafka;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Objects;

public class HeadersWrapper implements Headers {
    private final Headers headers;

    public HeadersWrapper(final Headers headers) {
        this.headers = headers;
    }

    @Override
    public Headers add(final Header header) throws IllegalStateException {
        return headers.add(header);
    }

    @Override
    public Headers add(final String key, final byte[] value) throws IllegalStateException {
        return headers.add(key, value);
    }

    @Override
    public Headers remove(final String key) throws IllegalStateException {
        return headers.remove(key);
    }

    @Override
    public Header lastHeader(final String key) {
        return headers.lastHeader(key);
    }

    @Override
    public Iterable<Header> headers(final String key) {
        return headers.headers(key);
    }

    @Override
    public Header[] toArray() {
        return headers.toArray();
    }

    @NotNull
    @Override
    public Iterator<Header> iterator() {
        final Iterator<Header> iterator = headers.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Header next() {
                Header header = iterator.next();
                if (header.value() != null) return header;
                return new RecordHeader(header.key(), new byte[0]);
            }
        };
    }

    @Override
    public int hashCode() {
        return headers != null ? headers.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeadersWrapper headers1 = (HeadersWrapper) o;
        return Objects.equals(headers, headers1.headers);
    }
}
